package zte.MBA.data.storage

import javax.swing.text.html.parser.Entity

import sun.plugin2.main.server.AppletID

import scala.actors.migration.Timeout
import scala.concurrent.{Await, Channel, Future, ExecutionContext}
import scala.concurrent.duration.Duration
import org.joda.time.DateTime

import scala.reflect.internal.Required

//DAO的基trait，不经过spark，直接返回[[Event]]
trait LEvents {
  val defaultTimeout = Duration(60, "seconds")

  /** 初始化特定AppID和渠道的Event存储
    * 当一个App第一次被创建时，会调用这个方法。
    *
    * @param appId App ID
    * @param channelId 可选的渠道ID
    * @return 初始化成功返回true；失败，false
    */
  def init(appId: Int, channelId: Option[Int] = None): Boolean

  /**
   * 关闭Event存储接口对象，关闭数据库连接，释放资源等
   */
  def close(): Unit

  /**
   * 插入一个[[Event]]， 非阻塞
   *
   * @param event 待插入[[Event]]
   * @param appId 待插入[[Event]]的appid
   */
  def futureInsert(event: Event, appId: Int)(implicit ec: ExecutionContext):
    Future[String] = futureInsert(event, appId, None)

  /**
   * 插入一个[[Event]]， 非阻塞
   *
   * @param event 待插入[[Event]]
   * @param appId 待插入[[Event]]的appid
   * @param channelId 待插入[[Event]]的channelid, 可选
   */
  def futureInsert(
    event: Event, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[String]

  /**
   * 获取一个特定的[[Event]]， 非阻塞
   *
   * @param eventId [[Event]]的ID
   * @param appId 待获取[[Event]]的appid
   */
  def futureGet(eventId: String, appId: Int)(implicit ec: ExecutionContext):
    Future[Option[Event]] = futureGet(eventId, appId, None)

  /**
   * 获取一个特定的[[Event]]， 非阻塞
   *
   * @param eventId [[Event]]的ID
   * @param appId 待获取[[Event]]的appid
   * @param channelId 待获取[[Event]]的channelid, 可选
   */
  def futureGet(
      eventId: String,
      appId: Int,
      channelId: Option[Int]
    )(implicit ec: ExecutionContext): Future[Option[Event]]

  /**
   * 删除一个特定的[[Event]]， 非阻塞
   *
   * @param eventId [[Event]]的ID
   * @param appId 待删除[[Event]]的appid
   */
  def futureDelete(eventId: String, appId: Int)(implicit ec: ExecutionContext):
    Future[Boolean] = futureDelete(eventId, appId, None)

  /**
   * 删除一个特定的[[Event]]， 非阻塞
   *
   * @param eventId [[Event]]的ID
   * @param appId 待删除[[Event]]的appid
   * @param channelId 待删除[[Event]]的channelid, 可选
   */
  def futureDelete(
      eventId: String,
      appId: Int,
      channelId: Option[Int]
    )(implicit ec: ExecutionContext): Future[Boolean]

  /**
   * 从数据库中读取并返回一个[[Event]]的集合的迭代器Future
   *
   * @param appId 返回此appId的events
   * @param channelId 返回此channel的events 可选
   * @param startTime 返回满足如下条件的events eventTime >= startTime
   * @param untilTime 返回满足如下条件的events eventTime < untilTime
   * @param entityType 返回这种entityType的events
   * @param entityId 返回这种entityId的events
   * @param eventNames 返回匹配eventNames中的任何一个name的events
   * @param targetEntityType 返回这种targetEntityType的events：
   *   - None： 没有targetEntityType约束
   *   - Some(None)： 这个event没有targetEntityType
   *   - Some(Some(x)): targetEntityType匹配x
   * @param targetEntityID 返回这种targetEntityId的events：
   *   - None： 没有targetEntityId约束
   *   - Some(None)： 这个event没有targetEntityId
   *   - Some(Some(x)): targetEntityId匹配x
   * @param limit events数目的上限，None或Some(-1)表示没有上限， 获取全部
   * @param reversed 逆序
   *   - 默认 最旧最前
   *   - Some(true) 最新最前
   * @param ec ExecutionContext
   * @param return Future[Iterator[Event]]
   */
  def futureFind(
      appId: Int,
      channelId: Option[Int] = None,
      startTime: Option[DateTime] = None,
      untilTime: Option[DateTime] = None,
      entityType: Option[String] = None,
      entityId: Option[String] = None,
      eventNames: Option[Seq[String]] = None,
      targetEntityId: Option[Option[String]] = None,
      targetEntityType: Option[Option[String]] = None,
      limit: Option[Int] = None,
      reversed: Option[Boolean] = None
    )(implicit ec: ExecutionContext): Future[Iterator[Event]]

  /** 基于如下events对实体的属性进行聚合:
    * \$set, \$unset, \$delete events.
    * 返回实体ID到实体属性的Map的Future.
    *
    * @param appId 在这个appID的events中聚合
    * @param channelId 在这个channelID的events中聚合 默认为None
    * @param entityType 待聚合实体的实体类型
    * @param startTime events约束 eventTime >= startTime
    * @param untilTime events约束 eventTime < untilTime
    * @param required 只保留定义了required中全部属性的Entity对应的event
    * @param ec ExecutionContext
    * @return Future[Map[String, PropertyMap]]
    */
  private[MBA] def futureAggregateProperties(
    appId: Int,
    channelId: Option[Int] = None,
    entityType: String,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    required: Option[Seq[String]] = None)(implicit ec: ExecutionContext):
    Future[Map[String, PropertyMap]] = {
      futureFind(
        appId = appId,
        channelId = channelId,
        startTime = startTime,
        untilTime = untilTime,
        entityType = Some(entityType),
        eventNames = Some(LEventAggregator.eventNames)
      ).map{
        eventIt =>
          val dm = LEventAggregator.aggregateProperties(eventIt)
          if (required.isDefined) {
            dm.filter {
              case (k, v) =>
                required.get.map(v.contains(_)).reduce(_&&_)
            }
          } else dm
      }
  }

  /** 基于如下events对实体的属性进行聚合:（entityType + entityId）
    * \$set, \$unset, \$delete events.
    * 返回实体ID到实体属性的Map的Future.
    *
    * @param appId 在这个appID的events中聚合
    * @param channelId 在这个channelID的events中聚合 默认为None
    * @param entityType 待聚合实体的实体类型
    * @param startTime events约束 eventTime >= startTime
    * @param untilTime events约束 eventTime < untilTime
    * @param entityId 实体类型id
    * @param ec ExecutionContext
    * @return Future[Option[PropertyMap]]
    */
  private[MBA] def futureAggregatePropertiesOfEntity(
    appId: Int,
    channelId: Option[Int] = None,
    entityType: String,
    entityId: String,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None)(implicit ec: ExecutionContext):
    Future[Option[PropertyMap]] = {
      futureFind(
        appId = appId,
        channelId = channelId,
        startTime = startTime,
        untilTime = untilTime,
        entityType = entityType,
        entityId = entityId,
        eventNames = Some(LEventAggregator.eventNames)
      ).map{
        eventIt =>
          LEventAggregator.aggregatePropertiesSingle(eventIt)
      }
  }

  //阻塞版本
  private[MBA] def insert(
    event: Event,
    appId: Int,
    channelId: Option[Int] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    String = {
    Await.result(futureInsert(event, appId, channelId), timeout)
  }

  private[MBA] def get(
    eventId: String,
    appId: Int,
    channelId: Option[Int] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    Option[Event] = {
    Await.result(futureGet(eventId, appId, channelId), timeout)
  }

  private[MBA] def delete(
    eventId: String,
    appId: Int,
    channelId: Option[Int] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    Boolean = {
    Await.result(futureDelete(eventId, appId, channelId), timeout)
  }

  /**
   * 读取数据库，返回events迭代器
   *
   */
  private[MBA] def find(
    appId: Int,
    channelId: Option[Int] = None,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    entityType: Option[String] = None,
    entityId: Option[String] = None,
    eventNames: Option[Seq[String]] = None,
    targetEntityId: Option[Option[String]] = None,
    targetEntityType: Option[Option[String]] = None,
    limit: Option[Int] = None,
    reversed: Option[Boolean] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    Iterator[Event] = {
    Await.result(futureFind(
      appId = appId,
      channelId = channelId,
      startTime = startTime,
      untilTime = untilTime,
      entityType = entityType,
      entityId = entityId,
      eventNames = eventNames,
      targetEntityType = targetEntityType,
      targetEntityId = targetEntityId,
      limit = limit,
      reversed = reversed), timeout)
  }

  private[MBA] def aggregateProperties(
    appId: Int,
    channelId: Option[Int] = None,
    entityType: String,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    required: Option[Seq[String]] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    Map[String, PropertyMap] = {
    Await.result(futureAggregateProperties(
      appId = appId,
      channelId = channelId,
      entityType = entityType,
      startTime = startTime,
      untilTime = untilTime,
      required = required), timeout)
  }

  private[MBA] def aggregatePropertiesOfEntity(
    appId: Int,
    channelId: Option[Int] = None,
    entityType: String,
    entityId: String,
    startTime: Option[DateTime] = None,
    untilTime: Option[DateTime] = None,
    timeout: Duration = defaultTimeout)(implicit ec: ExecutionContext):
    Option[PropertyMap] = {
      Await.result(futureAggregatePropertiesOfEntity(
      appId = appId,
      channelId = channelId,
      entityType = entityType,
      entityId = entityId,
      startTime = startTime,
      untilTime = untilTime), timeout)
  }

}
