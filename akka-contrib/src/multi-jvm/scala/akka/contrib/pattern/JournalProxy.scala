/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.pattern

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated

object JournalProxy {
  case class UseJournal(other: ActorRef)
}

/**
 * Journal for testing in distributed environment.
 * Delegates to a potentially remote journal.
 */
class JournalProxy extends Actor {
  import JournalProxy._
  var otherJournal: ActorRef = context.system.deadLetters

  def receive = {
    case UseJournal(other) ⇒
      otherJournal = other
      context watch otherJournal
    case Terminated(_) ⇒
      context stop self
    case msg ⇒
      otherJournal forward msg
  }

}