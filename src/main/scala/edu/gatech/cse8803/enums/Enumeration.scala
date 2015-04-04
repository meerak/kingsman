package edu.gatech.cse8803.enums

object Enumerations{
sealed trait Relation { def r_type: String }
case object ISA extends Relation { val r_type = "IS-A" }
case object CHILD extends Relation { val r_type = "CHILD" }
}