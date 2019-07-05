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
    val theOwner= typeSignature.typeSymbol
    val baseClass = typeSignature.baseClasses
    var fields = baseClass.flatMap(i=>i.asClass.typeSignature.decls).filter(i=>i.isTerm && (i.asTerm.isVar|| i.asTerm.isVal)).map(i=>i.asTerm)
    // fields = typeSignature.decls.filter(i=>i.isTerm&&(i.asTerm.isVal || i.asTerm.isVar)).map(_.asTerm).toList


    fields.foreach(i=>{

      println(s"${i.name}-${i.owner}-${i.isModule}-${i.isPrivate}-${i.isPrivateThis}-${i.isOverloaded}-${i.isImplementationArtifact}-${i.isCaseAccessor}")

    })
    val gg = fields.groupBy(_.name.decodedName.toString.trim)
    val z= fields.groupBy(_.name.decodedName.toString.trim).map(i=>{
      val f = if(i._2.size>1){
        i._2.filter(_.owner==theOwner)
      }else{
        i._2
      }
      f
    }).flatMap(_.iterator).toList

    val instance = ctorm()
    val ref = classMirror.reflect(instance)

    z foreach {f=>{
      println(s"${f}-${f.isImplementationArtifact}-${f.isPrivateThis}-${f.isCaseAccessor}")
      try {
        val nf = ref.reflectField(f)

        nf.set(f.name.decodedName.toString + Random.nextInt(100))
      } catch {
        case e:Exception =>


          e.printStackTrace()
      }
    }}
    println(instance)
  }


  test("constructor"){
    println("====")
    val constructorSymbol = typeSignature.decl(ru.termNames.CONSTRUCTOR)

   val z =   constructorSymbol   .filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty)
        .asMethod

    println(constructorSymbol)
    val str = "strZs"


  }
  test("list"){
    val parr =Array(10,11,12)
    val arr = Array(1,2,3,4,5,6)
    val z = arr.padTo(arr.length+parr.length,parr)
    println(z.mkString(","))
  }
}
