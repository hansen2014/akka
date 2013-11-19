/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.pattern

import language.postfixOps
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

import akka.actor.ActorIdentity
import akka.actor.Identify
import akka.actor.PoisonPill
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.persistence.EventsourcedProcessor
import akka.persistence.Persistence
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._
import akka.testkit.TestEvent.Mute

object ClusterShardingSpec extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")
  val sixth = role("sixth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.remote.log-remote-lifecycle-events = off
    akka.cluster.auto-down-unreachable-after = 0s
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem-proxy"
    akka.persistence.journal.inmem-proxy.class = "akka.contrib.pattern.JournalProxy"
    """))

  nodeConfig(controller) {
    ConfigFactory.parseString("""akka.persistence.journal.plugin = "akka.persistence.journal.inmem"""")
  }

  case object Increment
  case object Decrement
  case class Get(counterId: Long)
  case class CounterChanged(delta: Int)
  class Counter extends EventsourcedProcessor {

    override def processorId = "counter-" + self.path.name

    var count = 0

    def updateState(event: CounterChanged): Unit =
      count += event.delta

    override def receiveReplay: Receive = {
      case evt: CounterChanged ⇒ updateState(evt)
    }

    override def receiveCommand: Receive = {
      case Increment ⇒ persist(CounterChanged(+1))(updateState)
      case Decrement ⇒ persist(CounterChanged(-1))(updateState)
      case Get(_)    ⇒ sender ! count
    }
  }

  case class AggregateRootEnvelope(id: Long, payload: Any)

  val aggregateRootIdExtractor: Repository.IdExtractor = {
    case AggregateRootEnvelope(id, payload) ⇒ (id.toString, payload)
    case msg @ Get(id)                      ⇒ (id.toString, msg)
  }

  val aggregateRootShardResolver: Repository.ShardResolver = msg ⇒ msg match {
    case AggregateRootEnvelope(id, _) ⇒ (id % 10).toString
    case Get(id)                      ⇒ (id % 10).toString
  }

}

class ClusterShardingMultiJvmNode1 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode2 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode3 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode4 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode5 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode6 extends ClusterShardingSpec
class ClusterShardingMultiJvmNode7 extends ClusterShardingSpec

class ClusterShardingSpec extends MultiNodeSpec(ClusterShardingSpec) with STMultiNodeSpec with ImplicitSender {
  import ClusterShardingSpec._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      createCoordinator()
    }
    enterBarrier(from.name + "-joined")
  }

  def createCoordinator(): Unit = {
    system.actorOf(ClusterSingletonManager.props(
      singletonProps = ShardCoordinator.props(rebalanceThreshold = 2),
      singletonName = "singleton",
      terminationMessage = PoisonPill,
      role = None),
      name = "sharding")
  }

  lazy val repository = system.actorOf(Repository.props(
    aggregateRootProps = Props[Counter],
    role = None,
    coordinatorPath = "/user/sharding/singleton",
    idExtractor = aggregateRootIdExtractor,
    shardResolver = aggregateRootShardResolver),
    name = "repository")

  "Cluster sharding" must {

    "setup shared journal" in {
      // start the Persistence extension
      Persistence(system)
      enterBarrier("peristence-started")

      // FIXME this path will change to /system
      runOn(first, second, third, fourth, fifth, sixth) {
        system.actorSelection(node(controller) / "user" / "journal") ! Identify(None)
        val sharedJournal = expectMsgType[ActorIdentity].ref.get
        system.actorSelection("/user/journal") ! JournalProxy.UseJournal(sharedJournal)
      }

      enterBarrier("after-1")
    }

    "work in single node cluster" in within(20 seconds) {
      join(first, first)

      runOn(first) {
        repository ! AggregateRootEnvelope(1, Increment)
        repository ! AggregateRootEnvelope(1, Increment)
        repository ! AggregateRootEnvelope(1, Increment)
        repository ! AggregateRootEnvelope(1, Decrement)
        repository ! Get(1)
        expectMsg(2)
      }

      enterBarrier("after-2")
    }

    "use second node" in within(20 seconds) {
      join(second, first)

      runOn(second) {
        repository ! AggregateRootEnvelope(2, Increment)
        repository ! AggregateRootEnvelope(2, Increment)
        repository ! AggregateRootEnvelope(2, Increment)
        repository ! AggregateRootEnvelope(2, Decrement)
        repository ! Get(2)
        expectMsg(2)
      }
      enterBarrier("second-update")
      runOn(first) {
        repository ! AggregateRootEnvelope(2, Increment)
        repository ! Get(2)
        expectMsg(3)
        lastSender.path must be(node(second) / "user" / "repository" / "2")
      }
      enterBarrier("first-update")

      runOn(second) {
        repository ! Get(2)
        expectMsg(3)
        lastSender.path must be(repository.path / "2")
      }

      enterBarrier("after-3")
    }

    "failover shards on crashed node" in within(30 seconds) {
      // mute logging of deadLetters during shutdown of systems
      if (!log.isDebugEnabled)
        system.eventStream.publish(Mute(DeadLettersFilter[Any]))
      enterBarrier("logs-muted")

      runOn(controller) {
        testConductor.exit(second, 0).await
      }
      enterBarrier("crash-second")

      runOn(first) {
        val probe = TestProbe()
        awaitAssert {
          within(1.second) {
            repository.tell(Get(2), probe.ref)
            probe.expectMsg(3)
            probe.lastSender.path must be(repository.path / "2")
          }
        }
      }

      enterBarrier("after-4")
    }

    "use third and fourth node" in within(15 seconds) {
      join(third, first)
      join(fourth, first)

      runOn(third) {
        for (_ ← 1 to 10)
          repository ! AggregateRootEnvelope(3, Increment)
        repository ! Get(3)
        expectMsg(10)
      }
      enterBarrier("third-update")

      runOn(fourth) {
        for (_ ← 1 to 20)
          repository ! AggregateRootEnvelope(4, Increment)
        repository ! Get(4)
        expectMsg(20)
      }
      enterBarrier("fourth-update")

      runOn(first) {
        repository ! AggregateRootEnvelope(3, Increment)
        repository ! Get(3)
        expectMsg(11)
        lastSender.path must be(node(third) / "user" / "repository" / "3")

        repository ! AggregateRootEnvelope(4, Increment)
        repository ! Get(4)
        expectMsg(21)
        lastSender.path must be(node(fourth) / "user" / "repository" / "4")
      }
      enterBarrier("first-update")

      runOn(third) {
        repository ! Get(3)
        expectMsg(11)
        lastSender.path must be(repository.path / "3")
      }

      runOn(fourth) {
        repository ! Get(4)
        expectMsg(21)
        lastSender.path must be(repository.path / "4")
      }

      enterBarrier("after-5")
    }

    "recover coordinator state after coordinator crash" in within(60 seconds) {
      join(fifth, fourth)

      runOn(controller) {
        testConductor.exit(first, 0).await
      }
      enterBarrier("crash-first")

      runOn(fifth) {
        val probe3 = TestProbe()
        awaitAssert {
          within(1.second) {
            repository.tell(Get(3), probe3.ref)
            probe3.expectMsg(11)
            probe3.lastSender.path must be(node(third) / "user" / "repository" / "3")
          }
        }
        val probe4 = TestProbe()
        awaitAssert {
          within(1.second) {
            repository.tell(Get(4), probe4.ref)
            probe4.expectMsg(21)
            probe4.lastSender.path must be(node(fourth) / "user" / "repository" / "4")
          }
        }

      }

      enterBarrier("after-6")
    }

    "rebalance to nodes with less shards" in within(30 seconds) {

      runOn(fourth) {
        // third, fourth and fifth are still alive
        // shards 3 and 4 are already allocated
        // make sure shards 1 and 2 (previously on crashed first) are allocated
        repository ! Get(1)
        expectMsg(2)
        repository ! Get(2)
        expectMsg(3)

        // add more shards, which should later trigger rebalance to new node sixth 
        for (n ← 5 to 10)
          repository ! AggregateRootEnvelope(n, Increment)

        for (n ← 5 to 10) {
          repository ! Get(n)
          expectMsg(1)
        }
      }
      enterBarrier("more-added")

      join(sixth, third)

      runOn(sixth) {
        awaitAssert {
          val probe = TestProbe()
          within(3.seconds) {
            var count = 0
            for (n ← 1 to 10) {
              repository.tell(Get(n), probe.ref)
              probe.expectMsgType[Int]
              if (probe.lastSender.path == repository.path / n.toString)
                count += 1
            }
            count must be(2)
          }
        }
      }

      enterBarrier("after-7")

    }

  }
}
