package zte.MBA.data.storage

import org.json4s._
import org.json4s.native.JsonMethods.parse

import scala.collection.{GenTraversableOnce, JavaConversions}

case class DataMapException(msg: String, cause: Exception)
  extends Exception(msg, cause) {
  def this(msg: String) = this(msg, null)
}


/**DataMap �����洢Event��Entity�����ԡ�
 * DataMap��Key����������Value����Ӧ��Jsonֵ
 * [[get]] �������ǿ�Ƶ�����ֵ
 * [[getOpt]] ������ÿ�ѡ����ֵ
 * @param fields Map of property name to JValue
 */
class DataMap (
  val fields: Map[String, JValue]//��������
) extends Serializable {
  @transient lazy implicit private val formats = DefaultFormats +
    new DateTimeJson4sSupport.Serializer

  /**
   * ����Ƿ������Ӧ��������
   * @param name ϣ�����ڵ�������
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
   * ��ȡĳ��ǿ�����Ե�ֵ����������Բ����ڽ��׳��쳣
   * @param name ������
   * @tparam T ����ֵ������
   * @return ���ص�����ֵ��������ΪT
   */
  def get[T: Manifest](name: String): T = {
    require(name)
    fields(name) match {
      case JNull => throw new DataMapException( //ƥ��ֵ
        s"The required field $name cannot be null.")
      case x: JValue => x.extract[T] //ƥ������
    }
  }

  /**
   * ��ȡĳ����ѡ���Ե�ֵ����������Բ����ڣ��򷵻�None
   * @param name ������
   * @tparam T ����ֵ������
   * @return ���ص�����ֵ��������ΪT
   */
  def getOpt[T: Manifest](name: String): Option[T] = {//Manifest ����ʱ����������Ϣ
    fields.get(name).flatMap(_.extract[Option[T]])
  }

  /**
   * ��ȡĳ����ѡ���Ե�ֵ����������Բ����ڣ��򷵻�Ĭ������ֵ
   * @param name ������
   * @param default Ĭ�ϵ�����ֵ
   * @tparam T ����ֵ������
   * @return ���ص�����ֵ��������ΪT
   */
  def getOrElse[T: Manifest](name: String, default: T): T = {
    getOpt[T](name).getOrElse(default)
  }

  /**
   * Java�Ѻð汾
   * @param name
   * @param clazz ����ֵ���͵���
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
   * ����ĳ����������Ӧ������ֵlist��������Բ����ڣ��򷵻�null
   * @param name
   * @return ����ֵlist
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
   * ������DataMapƴ�ӳ�һ��DataMap
   * @param that ��ֵ
   * @return ��DataMap
   */
  def ++ (that: DataMap): DataMap = DataMap(this.fields ++ that.fields)

  /**
   * ����ֵMap���Ƴ�������ֵMap�е�Ԫ��
   * @param that ��ֵ
   * @return ��DataMap
   */
  def -- (that: GenTraversableOnce[String]): DataMap =
    DataMap(this.fields -- that)

  def isEmpty: Boolean = fields.isEmpty

  /**
   * ��DataMap����������������ΪSet
   * @return Set
   */
  def keySet: Set[String] = this.fields.keySet

  /**
   * ��DataMapת��ΪList
   * @return
   */
  def toList(): List[(String, JValue)] = fields.toList

  /**
   * ��DataMapת��Ϊһ��JObject
   * @return
   */
  def toJObject(): JObject = JObject(toList())

  /**
   *��DataMapת��������T��case class
   * @tparam T
   * @return ����ΪT�Ķ���
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
 * DataMap��İ������
 */
object DataMap {

  /**
   * ������DataMap
   * @return һ����DataMap
   */
  def apply(): DataMap = new DataMap(Map[String, JValue]())

  /**
   * ��һ��String��JValue��Map�д���һ��DataMap
   * @param fields һ��String��JValue��Map
   * @return һ��DataMap�� fields������ʼ��
   */
  def apply(fields: Map[String, JValue]): DataMap = new DataMap(fields)

  /**
   * ��һ��JObject�д���һ��DataMap
   * @param jObj JObject
   * @return һ��DataMap�� fields������ʼ��
   */
  def apply(jObj: JObject): DataMap = {
    if (jObj == null) {
      apply() //��
    } else {
      new DataMap(jObj.obj.toMap) //��ȡfeilds
    }
  }

  /**
   * ��һ��JSON String�д���һ��DataMap
   * @param js
   * @return
   */
  def apply(js: String): DataMap = apply(parse(js).asInstanceOf[JObject])//��ת��ΪJObject


}
