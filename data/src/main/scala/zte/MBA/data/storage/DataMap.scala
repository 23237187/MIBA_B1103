package zte.MBA.data.storage

import org.json4s._
import org.json4s.native.JsonMethods.parse

import scala.collection.{GenTraversableOnce, JavaConversions}

case class DataMapException(msg: String, cause: Exception)
  extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}


/**DataMap 用来存储Event或Entity的属性。
 * DataMap的Key是属性名，Value是相应的Json值
 * [[get]] 方法获得强制的属性值
 * [[getOpt]] 方法获得可选属性值
 * @param fields Map of property name to JValue
 */
class DataMap (
  val fields: Map[String, JValue]//主体数据
) extends Serializable {
  @transient lazy implicit private val formats = DefaultFormats +
    new DateTimeJson4sSupport.Serializer

  /**
   * 检查是否存在相应的属性名
   * @param name 希望存在的属性名
   */
  def require(name: String): Unit = {
    if (!fields.contains(name)) {
      throw new DataMapException(s"The field $name is required.")
    }
  }

  def contains(name: String): Boolean = {
    fields.contains(name)
  }

  /**
   * 获取某个强制属性的值，如果该属性不存在将抛出异常
   * @param name 属性名
   * @tparam T 属性值的类型
   * @return 返回的属性值，其类型为T
   */
  def get[T: Manifest](name: String): T = {
    require(name)
    fields(name) match {
      case JNull => throw new DataMapException( //匹配值
        s"The required field $name cannot be null.")
      case x: JValue => x.extract[T] //匹配类型
    }
  }

  /**
   * 获取某个可选属性的值，如果该属性不存在，则返回None
   * @param name 属性名
   * @tparam T 属性值的类型
   * @return 返回的属性值，其类型为T
   */
  def getOpt[T: Manifest](name: String): Option[T] = {//Manifest 运行时数据类型信息
    fields.get(name).flatMap(_.extract[Option[T]])
  }

  /**
   * 获取某个可选属性的值，如果该属性不存在，则返回默认属性值
   * @param name 属性名
   * @param default 默认的属性值
   * @tparam T 属性值的类型
   * @return 返回的属性值，其类型为T
   */
  def getOrElse[T: Manifest](name: String, default: T): T = {
    getOpt[T](name).getOrElse(default)
  }

  /**
   * Java友好版本
   * @param name
   * @param clazz 属性值类型的类
   * @tparam T
   * @return
   */
  def get[T](name: String, clazz: java.lang.Class[T]): T = {
    val manifest = new Manifest[T] {
      override def erasure: Class[_] = clazz
      override def runtimeClass: Class[_] = clazz
    }

    fields.get(name) match {
      case None => null.asInstanceOf[T]
      case Some(JNull) => null.asInstanceOf[T]
      case Some(x) => x.extract[T](formats, manifest)
    }
  }

  /**
   * 返回某个属性名对应的属性值list，如果属性不存在，则返回null
   * @param name
   * @return 属性值list
   */
  def getStringList(name: String): java.util.List[String] = {
    fields.get(name) match {
      case None => null
      case Some(JNull) => null
      case Some(x) =>
        JavaConversions.seqAsJavaList(x.extract[List[String]](formats, manifest[List[String]]))
    }
  }

  /**
   * 将两个DataMap拼接成一个DataMap
   * @param that 右值
   * @return 新DataMap
   */
  def ++ (that: DataMap): DataMap = DataMap(this.fields ++ that.fields)

  /**
   * 从左值Map中移除所有右值Map中的元素
   * @param that 右值
   * @return 新DataMap
   */
  def -- (that: GenTraversableOnce[String]): DataMap =
    DataMap(this.fields -- that)

  def isEmpty: Boolean = fields.isEmpty

  /**
   * 将DataMap的所有属性名返回为Set
   * @return Set
   */
  def keySet: Set[String] = this.fields.keySet

  /**
   * 将DataMap转变为List
   * @return
   */
  def toList(): List[(String, JValue)] = fields.toList

  /**
   * 将DataMap转变为一个JObject
   * @return
   */
  def toJObject(): JObject = JObject(toList())

  /**
   *将DataMap转化成类型T的case class
   * @tparam T
   * @return 类型为T的对象
   */
  def extract[T: Manifest]: T = {
    toJObject().extract[T]
  }

  override
  def toString: String = s"DataMap($fields)"

  override
  def hashCode: Int = 41 + fields.hashCode

  override
  def equals(other: Any): Boolean = other match {
    case that: DataMap => that.canEqual(this) && this.fields.equals(that.fields)
    case _=> false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[DataMap]

}

/**
 * DataMap类的伴随对象
 */
object DataMap {

  /**
   * 创建空DataMap
   * @return 一个空DataMap
   */
  def apply(): DataMap = new DataMap(Map[String, JValue]())

  /**
   * 从一个String到JValue的Map中创建一个DataMap
   * @param fields 一个String到JValue的Map
   * @return 一个DataMap， fields经过初始化
   */
  def apply(fields: Map[String, JValue]): DataMap = new DataMap(fields)

  /**
   * 从一个JObject中创建一个DataMap
   * @param jObj JObject
   * @return 一个DataMap， fields经过初始化
   */
  def apply(jObj: JObject): DataMap = {
    if (jObj == null) {
      apply() //空
    } else {
      new DataMap(jObj.obj.toMap) //提取feilds
    }
  }

  /**
   * 从一个JSON String中创建一个DataMap
   * @param js
   * @return
   */
  def apply(js: String): DataMap = apply(parse(js).asInstanceOf[JObject])//先转化为JObject


}
