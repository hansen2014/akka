/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.pattern

import java.net.URLEncoder
import scala.collection.immutable
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.RootActorPath
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.persistence.EventsourcedProcessor

object Repository {

  def props(
    aggregateRootProps: Props,
    role: Option[String],
    coordinatorPath: String,
    idExtractor: Repository.IdExtractor,
    shardResolver: Repository.ShardResolver): Props =
    Props(classOf[Repository], aggregateRootProps, role, coordinatorPath, idExtractor, shardResolver)

  type AggregateRootId = String
  type ShardId = String
  type Msg = Any
  type IdExtractor = PartialFunction[Msg, (AggregateRootId, Msg)]
  type ShardResolver = Msg ⇒ ShardId

  private case object Retry

  class HandOffStopper(shard: String, replyTo: ActorRef, entities: Set[ActorRef]) extends Actor {
    import ShardCoordinator.ShardStopped

    entities.foreach { a ⇒
      context watch a
      a ! PoisonPill
    }

    var remaining = entities

    def receive = {
      case Terminated(ref) ⇒
        remaining -= ref
        if (remaining.isEmpty) {
          replyTo ! ShardStopped(shard)
          context stop self
        }
    }
  }
}

class Repository(
  aggregateRootProps: Props,
  role: Option[String],
  coordinatorPath: String,
  idExtractor: Repository.IdExtractor,
  shardResolver: Repository.ShardResolver) extends Actor with ActorLogging {

  import ShardCoordinator._
  import Repository._

  val cluster = Cluster(context.system)

  // sort by age, oldest first
  val ageOrdering = Ordering.fromLessThan[Member] { (a, b) ⇒ a.isOlderThan(b) }
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  var repositories = Map.empty[ActorRef, ShardId]
  var repositoryByShard = Map.empty[ShardId, ActorRef]
  var entities = Map.empty[ActorRef, ShardId]
  var entitiesByShard = Map.empty[ShardId, Set[ActorRef]]
  var shardBuffers = Map.empty[ShardId, Vector[(Msg, ActorRef)]]

  import context.dispatcher
  val retryTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Retry)

  // subscribe to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    retryTask.cancel()
  }

  def matchingRole(member: Member): Boolean = role match {
    case None    ⇒ true
    case Some(r) ⇒ member.hasRole(r)
  }

  def coordinatorSelection: Option[ActorSelection] =
    membersByAge.headOption.map(m ⇒ context.actorSelection(RootActorPath(m.address) + coordinatorPath))

  var coordinator: Option[ActorRef] = None

  def changeMembers(newMembers: immutable.SortedSet[Member]): Unit = {
    val before = membersByAge.headOption
    val after = newMembers.headOption
    membersByAge = newMembers
    if (before != after) {
      // FIXME change all log.info to log.debug
      log.info("Coordinator located at [{}]", after.map(_.address))
      coordinator = None
      register()
    }
  }

  def receive = {
    case state: CurrentClusterState ⇒
      changeMembers {
        immutable.SortedSet.empty(ageOrdering) ++ state.members.collect {
          case m if m.status == MemberStatus.Up && matchingRole(m) ⇒ m
        }
      }

    case MemberUp(m) ⇒
      if (matchingRole(m))
        changeMembers(membersByAge + m)

    case MemberRemoved(m, _) ⇒
      if (matchingRole(m))
        changeMembers(membersByAge - m)

    case ShardHome(shard, ref) ⇒
      log.info("Shard [{}] located at [{}]", shard, ref)
      repositoryByShard.get(shard) match {
        case Some(r) if r == context.self && ref != context.self ⇒
          throw new IllegalStateException(s"Unannounced change of shard [${shard}] from self to [${ref}]")
        case _ ⇒
      }
      repositoryByShard = repositoryByShard.updated(shard, ref)
      repositories = repositories.updated(ref, shard)
      if (ref != context.self)
        context.watch(ref)
      shardBuffers.get(shard) match {
        case Some(buf) ⇒
          buf.foreach {
            case (msg, snd) ⇒ deliverMessage(msg, snd)
          }
          shardBuffers -= shard
        case None ⇒
      }

    case Terminated(ref) ⇒
      if (coordinator.exists(_ == ref))
        coordinator = None
      else if (repositories.contains(ref)) {
        val shard = repositories(ref)
        repositoryByShard -= shard
        repositories -= ref
      } else if (entities.contains(ref)) {
        val shard = entities(ref)
        val newShardEntities = entitiesByShard(shard) - ref
        if (newShardEntities.isEmpty)
          entitiesByShard -= shard
        else
          entitiesByShard = entitiesByShard.updated(shard, newShardEntities)
        entities -= ref
        // FIXME support buffering during stop of aggregateRoot
      }

    case RegisterAck(coord) ⇒
      context.watch(coord)
      coordinator = Some(coord)
      requestShardBufferHomes()

    case Retry ⇒
      if (coordinator.isEmpty)
        register()
      else
        requestShardBufferHomes()

    case BeginHandOff(shard) ⇒
      log.info("BeginHandOff shard [{}]", shard)
      if (repositoryByShard.contains(shard)) {
        repositories -= repositoryByShard(shard)
        repositoryByShard -= shard
      }
      sender ! BeginHandOffAck(shard)

    case HandOff(shard) ⇒
      log.info("HandOff shard [{}]", shard)

      // must drop requests that came in between the BeginHandOff and now,
      // because they might be forwarded from other repositories and there
      // is a risk or message re-ordering otherwise
      // FIXME do we need to be even more strict?
      if (shardBuffers.contains(shard))
        shardBuffers -= shard

      if (entitiesByShard.contains(shard))
        context.actorOf(Props(classOf[HandOffStopper], shard, sender, entitiesByShard(shard)))
      else
        sender ! ShardStopped(shard)

    case msg if idExtractor.isDefinedAt(msg) ⇒
      deliverMessage(msg, sender)

  }

  def register(): Unit = {
    coordinatorSelection.foreach(_ ! Register(self))
  }

  def requestShardBufferHomes(): Unit = {
    shardBuffers.foreach {
      case (shard, _) ⇒ coordinator.foreach { c ⇒
        log.info("Retry request for shard [{}] homes", shard)
        c ! GetShardHome(shard)
      }
    }
  }

  def deliverMessage(msg: Any, snd: ActorRef): Unit = {
    val shard = shardResolver(msg)
    repositoryByShard.get(shard) match {
      case Some(ref) if ref == context.self ⇒
        val (id, m) = idExtractor(msg)
        val name = URLEncoder.encode(id, "utf-8")
        val aggregateRoot = context.child(name).getOrElse {
          log.info("Starting aggregate root [{}] in shard [{}]", id, shard)
          val a = context.watch(context.actorOf(aggregateRootProps, name))
          entities = entities.updated(a, shard)
          entitiesByShard = entitiesByShard.updated(shard, entitiesByShard.getOrElse(shard, Set.empty) + a)
          a
        }
        log.info("Message for shard [{}] sent to aggregate root", shard)
        aggregateRoot.tell(m, snd)
      case Some(ref) ⇒
        log.info("Forwarding request for shard [{}] to [{}]", shard, ref)
        ref.tell(msg, snd)
      case None ⇒
        if (!shardBuffers.contains(shard)) {
          log.info("Request shard [{}] home", shard)
          coordinator.foreach(_ ! GetShardHome(shard))
        }
        val buf = shardBuffers.getOrElse(shard, Vector.empty)
        // FIXME limit buffer size
        shardBuffers = shardBuffers.updated(shard, buf :+ ((msg, sender)))

    }
  }
}

object ShardCoordinator {

  import Repository.ShardId

  def props(rebalanceThreshold: Int): Props = Props(classOf[ShardCoordinator], rebalanceThreshold)

  case class Register(repository: ActorRef)
  case class RegisterAck(coordinator: ActorRef)
  case class GetShardHome(shard: ShardId)
  case class ShardHome(shard: ShardId, ref: ActorRef)

  private case object Rebalance
  private case class RebalanceDone(shard: ShardId, ok: Boolean)
  case class BeginHandOff(shard: ShardId)
  case class BeginHandOffAck(shard: ShardId)
  case class HandOff(shard: ShardId)
  case class ShardStopped(shard: ShardId)

  // FIXME bug in watch during replay
  private case class DeferredWatch(ref: ActorRef)

  // DomainEvents
  sealed trait DomainEvent
  case class RepositoryRegistered(repository: ActorRef) extends DomainEvent
  case class RepositoryTerminated(repository: ActorRef) extends DomainEvent
  case class ShardHomeAllocated(shard: ShardId, repository: ActorRef) extends DomainEvent
  case class ShardHomeDeallocated(shard: ShardId) extends DomainEvent

  object State {
    val empty = State()
  }

  case class State private (
    // repository for each shard   
    val shards: Map[ShardId, ActorRef] = Map.empty,
    // shards for each repository
    val repositories: Map[ActorRef, Vector[ShardId]] = Map.empty) {

    def updated(event: DomainEvent): State = event match {
      case RepositoryRegistered(repository) ⇒
        copy(repositories = repositories.updated(repository, Vector.empty))
      case RepositoryTerminated(repository) ⇒
        copy(
          repositories = repositories - repository,
          shards = shards -- repositories(repository))
      case ShardHomeAllocated(shard, repository) ⇒
        copy(
          shards = shards.updated(shard, repository),
          repositories = repositories.updated(repository, repositories(repository) :+ shard))
      case ShardHomeDeallocated(shard) ⇒
        val repo = shards(shard)
        copy(
          shards = shards - shard,
          repositories = repositories.updated(repo, repositories(repo).filterNot(_ == shard)))
    }
  }

  class RebalanceWorker(shard: String, from: ActorRef, repositories: Set[ActorRef]) extends Actor {
    repositories.foreach(_ ! BeginHandOff(shard))
    var remaining = repositories

    import context.dispatcher
    context.system.scheduler.scheduleOnce(30.seconds, self, ReceiveTimeout)

    def receive = {
      case BeginHandOffAck(`shard`) ⇒
        remaining -= sender
        if (remaining.isEmpty) {
          from ! HandOff(shard)
          context.become(stoppingShard, discardOld = true)
        }
      case ReceiveTimeout ⇒ done(ok = false)
    }

    def stoppingShard: Receive = {
      case ShardStopped(shard) ⇒ done(ok = true)
      case ReceiveTimeout      ⇒ done(ok = false)
    }

    def done(ok: Boolean): Unit = {
      context.parent ! RebalanceDone(shard, ok)
      context.stop(self)
    }
  }

}

class ShardCoordinator(rebalanceThreshold: Int) extends EventsourcedProcessor with ActorLogging {
  import ShardCoordinator._
  import Repository.ShardId

  override def processorId = self.path.elements.mkString("/", "/", "")

  var persistentState = State.empty
  var rebalanceInProgress = Set.empty[ShardId]

  import context.dispatcher
  val rebalanceTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, Rebalance)

  override def postStop(): Unit = {
    super.postStop()
    rebalanceTask.cancel()
  }

  override def receiveReplay: Receive = {
    case evt @ RepositoryRegistered(repository) ⇒
      // FIXME bug in watch during replay
      // context.watch(repository)
      self ! DeferredWatch(repository)
      persistentState = persistentState.updated(evt)
    case evt @ RepositoryTerminated(repository) ⇒
      context.unwatch(repository)
      persistentState = persistentState.updated(evt)
    case evt: ShardHomeAllocated ⇒
      persistentState = persistentState.updated(evt)
    case evt: ShardHomeDeallocated ⇒
      persistentState = persistentState.updated(evt)
  }

  override def receiveCommand: Receive = {
    case Register(repository) ⇒
      log.info("Repository registered: [{}]", repository)
      if (persistentState.repositories.contains(repository))
        sender ! RegisterAck(self)
      else
        persist(RepositoryRegistered(repository)) { evt ⇒
          persistentState = persistentState.updated(evt)
          context.watch(repository)
          sender ! RegisterAck(self)
        }

    // FIXME bug in watch during replay
    case DeferredWatch(repository) ⇒
      log.info("DeferredWatch repository: [{}]", repository)
      context watch repository

    case Terminated(ref) ⇒
      log.info("Repository terminated: [{}]", ref)
      if (persistentState.repositories.contains(ref)) {
        persist(RepositoryTerminated(ref)) { evt ⇒
          persistentState = persistentState.updated(evt)
        }
      }

    case GetShardHome(shard) ⇒
      if (!rebalanceInProgress.contains(shard)) {
        persistentState.shards.get(shard) match {
          case Some(ref) ⇒ sender ! ShardHome(shard, ref)
          case None ⇒
            if (persistentState.repositories.nonEmpty) {
              val (repoWithLeastShards, _) = persistentState.repositories.minBy {
                case (_, v) ⇒ v.size
              }
              persist(ShardHomeAllocated(shard, repoWithLeastShards)) { evt ⇒
                persistentState = persistentState.updated(evt)
                log.info("Shard [{}] allocated at [{}]", evt.shard, evt.repository)
                sender ! ShardHome(evt.shard, evt.repository)
              }
            }
        }
      }

    case Rebalance ⇒
      // FIXME perhaps allow several rebalance in progress
      if (persistentState.repositories.nonEmpty && rebalanceInProgress.isEmpty) {
        val (repoWithLeastShards, leastShards) = persistentState.repositories.minBy {
          case (_, v) ⇒ v.size
        }
        val (repoWithMostShards, mostShards) = persistentState.repositories.maxBy {
          case (_, v) ⇒ v.size
        }
        if (mostShards.size - leastShards.size >= rebalanceThreshold) {
          val shard = mostShards.last
          rebalanceInProgress += shard
          log.info("Rebalance shard [{}] from [{}]", shard, repoWithMostShards)
          context.actorOf(Props(classOf[RebalanceWorker], shard, repoWithMostShards, persistentState.repositories.keySet))
        }

      }

    case RebalanceDone(shard, ok) ⇒
      rebalanceInProgress -= shard
      log.info("Rebalance shard [{}] done [{}]", shard, ok)
      if (ok) persist(ShardHomeDeallocated(shard)) { evt ⇒
        persistentState = persistentState.updated(evt)
        log.info("Shard [{}] deallocated", evt.shard)
      }

  }

}

// FIXME create an extension for the ShardCoordinator

// FIXME perhaps create a RepositoryProxy that can be used on frontend nodes, only delegating to backend Repositories
