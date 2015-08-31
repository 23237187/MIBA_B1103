package zte.MBA.workflow

import com.twitter.bijection.Injection
import com.twitter.chill.{KryoBase, ScalaKryoInstantiator, KryoInjection}
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer

class CreateServer {

}

class KryoInstantiator(classLoader: ClassLoader) extends ScalaKryoInstantiator {
  override
  def newKryo(): KryoBase = {
    val kryo = super.newKryo()
    kryo.setClassLoader(classLoader)
    SynchronizedCollectionsSerializer.registerSerializers(kryo)
    kryo
  }
}

object KryoInstantiator extends Serializable {
  def newKryoInjection: Injection[Any, Array[Byte]] = {
    val kryoInstantiator = new KryoInstantiator(getClass.getClassLoader)
    KryoInjection.instance(kryoInstantiator)
  }
}
