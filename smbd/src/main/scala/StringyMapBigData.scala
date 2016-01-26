// Copyright (C) 2015 Sam Halliday
// License: http://www.apache.org/licenses/LICENSE-2.0
/**
 * TypeClass (api/impl/syntax) for marshalling objects into
 * `java.util.HashMap<String,Object>` (yay, big data!).
 */
package s4m.smbd

import shapeless._, labelled.{ field, FieldType }

/**
 * This exercise involves writing tests, only a skeleton is provided.
 *
 * - Exercise 1.1: derive =BigDataFormat= for sealed traits.
 * - Exercise 1.2: define identity constraints using singleton types.
 */
package object api {
  type StringyMap = java.util.HashMap[String, Any]
  type BigResult[T] = Either[String, T] // aggregating errors doesn't add much
}

package api {
  trait BigDataFormat[T] {
    def label: String
    def toProperties(t: T): StringyMap
    def fromProperties(m: StringyMap): BigResult[T]
  }

  trait SPrimitive[V] {
    def toValue(v: V): Any
    def fromValue(v: Any): V
  }

  // EXERCISE 1.2
  trait BigDataFormatId[T, V] {
    def key: String
    def value(t: T): V
  }
}

package object impl {
  import api._

  // EXERCISE 1.1 goes here
  implicit object IntSPrimitive extends SPrimitive[Int] {
    def toValue(v: Int): Any = v
    def fromValue(v: Any): Int = v.asInstanceOf[Int]
  }
  implicit object StringSPrimitive extends SPrimitive[String] {
    def toValue(v: String): Any = v
    def fromValue(v: Any): String = v.asInstanceOf[String]
  }
  implicit object BooleanSPrimitive extends SPrimitive[Boolean] {
    def toValue(v: Boolean): Any = v
    def fromValue(v: Any): Boolean = v.asInstanceOf[Boolean]
  }
  implicit object DoubleSPrimitive extends SPrimitive[Double] {
    def toValue(v: Double): Any = v
    def fromValue(v: Any): Double = v.asInstanceOf[Double]
  }
  implicit object hNilBigDataFormat extends BigDataFormat[HNil] {
    def label: String = "EMPTY"
    def toProperties(t: HNil): StringyMap = new StringyMap()
    def fromProperties(m: StringyMap): BigResult[HNil] = Right(HNil)
  }
  implicit def hListBigDataFormat[Key <: Symbol, Value, Remaining <: HList](
    implicit
    key: Witness.Aux[Key],
    sp: SPrimitive[Value],
    lazyBdft: Lazy[BigDataFormat[Remaining]]
  ): BigDataFormat[FieldType[Key, Value] :: Remaining] = new BigDataFormat[FieldType[Key, Value] :: Remaining] {
    val bdft = lazyBdft.value
    def label: String = key.value.name
    def toProperties(hlist: FieldType[Key, Value] :: Remaining): StringyMap = {
      val m = bdft.toProperties(hlist.tail)
      m.put(label, sp.toValue(hlist.head))
      m
    }
    def fromProperties(m: StringyMap): BigResult[FieldType[Key, Value] :: Remaining] = {
      val head = sp.fromValue(m.get(key.value.name))
      m.remove(key.value.name)
      bdft.fromProperties(m) match {
        case Left(err) => Left(err)
        case Right(tail) => Right(field[Key](head) :: tail)
      }
    }
  }
  implicit object cNilBigDataFormat extends BigDataFormat[CNil] {
    def label: String = ???
    def toProperties(t: CNil): StringyMap = ???
    def fromProperties(m: StringyMap): BigResult[CNil] = ???
  }
  implicit def coproductBigDataFormat[Name <: Symbol, Head, Tail <: Coproduct](
    implicit
    key: Witness.Aux[Name],
    sp: SPrimitive[Head],
    lazyBdft: Lazy[BigDataFormat[Tail]]
  ): BigDataFormat[FieldType[Name, Head] :+: Tail] = new BigDataFormat[FieldType[Name, Head] :+: Tail] {
    val bdft = lazyBdft.value
    def label: String = key.value.name
    def toProperties(lr: FieldType[Name, Head] :+: Tail): StringyMap = lr match {
      case Inl(head) =>
        val m = new StringyMap()
        m.put(label, sp.toValue(head))
        m
      case Inr(tail) => bdft.toProperties(tail)
    }
    def fromProperties(m: StringyMap): BigResult[FieldType[Name, Head] :+: Tail] = {
      if (m.containsKey(label)) {
        Right(Inl(field[Name](sp.fromValue(m.get(label)))))
      } else {
        bdft.fromProperties(m) match {
          case Left(err) => Left(err)
          case Right(tail) => Right(Inr(tail))
        }
      }
    }
  }

  implicit def familyBigDataFormat[T, Repr](
    implicit
    gen: LabelledGeneric.Aux[T, Repr],
    lazySg: Lazy[BigDataFormat[Repr]],
    tpe: Typeable[T]
  ): BigDataFormat[T] = new BigDataFormat[T] {
    val sg = lazySg.value

    def label: String = sg.label
    def toProperties(t: T): StringyMap = sg.toProperties(gen.to(t))
    def fromProperties(m: StringyMap): BigResult[T] = sg.fromProperties(m) match {
      case Left(err) => Left(err)
      case Right(res) => Right(gen.from(res))
    }
  }
}

package impl {
  import api._

  // EXERCISE 1.2 goes here
}

package object syntax {
  import api._

  implicit class RichBigResult[R](val e: BigResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) => throw new IllegalArgumentException(error.mkString(","))
      case Right(r) => r
    }
  }

  /** Syntactic helper for serialisables. */
  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def label(implicit s: BigDataFormat[T]): String = s.label
    def toProperties(implicit s: BigDataFormat[T]): StringyMap = s.toProperties(t)
    def idKey[P](implicit lens: Lens[T, P]): String = ???
    def idValue[P](implicit lens: Lens[T, P]): P = lens.get(t)
  }

  implicit class RichProperties(val props: StringyMap) extends AnyVal {
    def as[T](implicit s: BigDataFormat[T]): T = s.fromProperties(props).getOrThrowError
  }
}
