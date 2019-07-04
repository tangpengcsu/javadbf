package com.linuxense.javadbf.spark

import org.scalatest.FunSuite

import scala.reflect.runtime.{universe => ru}
import scala.util.Random

class ReflectTest extends FunSuite{

  val clazz = classOf[V1]

  val classMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
  val classSymbol = classMirror.classSymbol(clazz)
  val reflectClass = classMirror.reflectClass(classSymbol)
  val typeSignature = reflectClass.symbol.typeSignature
  // val ctorC = typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
  val cons = typeSignature.decl(ru.termNames.CONSTRUCTOR).filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty).asMethod
  val ctorm = reflectClass.reflectConstructor(cons)



  test("fields"){


    val baseClass = typeSignature.baseClasses
    val fields = baseClass.flatMap(i=>i.asClass.typeSignature.decls).filter(i=>i.isTerm&& (i.asTerm.isVar|| i.asTerm.isVal)).map(i=>i.asTerm)

    fields.foreach(i=>println(i.name.decodedName))
  }

  test("relect class"){
    val baseClass = typeSignature.baseClasses
    val fields = baseClass.flatMap(i=>i.asClass.typeSignature.decls).filter(i=>i.isTerm&& (i.asTerm.isVar|| i.asTerm.isVal)).map(i=>i.asTerm)

    val instance = ctorm()
    val ref = classMirror.reflect(instance)

    fields foreach {f=>{
      val nf = ref.reflectField(f)

      nf.set(f.name.decodedName.toString + Random.nextInt(100))
    }}
    println(instance)
  }
}
