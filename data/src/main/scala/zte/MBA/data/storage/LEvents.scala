package zte.MBA.data.storage

import javax.swing.text.html.parser.Entity

import sun.plugin2.main.server.AppletID

import scala.actors.migration.Timeout
import scala.concurrent.{Await, Channel, Future, ExecutionContext}
import scala.concurrent.duration.Duration
import org.joda.time.DateTime

import scala.reflect.internal.Required

//DAO�Ļ�trait��������spark��ֱ�ӷ���[[Event]]
trait LEvents {
  val defaultTimeout = Duration(60, "seconds")

  /** ��ʼ���ض�AppID��������Event�洢
    * ��һ��App��һ�α�����ʱ����������������
    *
    * @param appId App ID
    * @param channelId ��ѡ������ID
    * @return ��ʼ���ɹ�����true��ʧ�ܣ�false
    */
  def init(appId: Int, channelId: Option[Int] = None): Boolean

  /**
   * �ر�Event�洢�ӿڶ��󣬹ر����ݿ����ӣ��ͷ���Դ��
   */
  def close(): Unit

  /**
   * ����һ��[[Event]]�� ������
   *
   * @param event ������[[Event]]
   * @param appId ������[[Event]]��appid
   */
  def futureInsert(event: Event, appId: Int)(implicit ec: ExecutionContext):
    Future[String] = futureInsert(event, appId, None)

  /**
   * ����һ��[[Event]]�� ������
   *
   * @param event ������[[Event]]
   * @param appId ������[[Event]]��appid
   * @param channelId ������[[Event]]��channelid, ��ѡ
   */
  def futureInsert(
    event: Event, appId: Int, channelId: Option[Int])(implicit ec: ExecutionContext): Future[String]

  /**
   * ��ȡһ���ض���[[Event]]�� ������
   *
   * @param eventId [[Event]]��ID
   * @param appId ����ȡ[[Event]]��appid
   */
  def futureGet(eventId: String, appId: Int)(implicit ec: ExecutionContext):
    Future[Option[Event]] = futureGet(eventId, appId, None)

  /**
   * ��ȡһ���ض���[[Event]]�� ������
   *
   * @param eventId [[Event]]��ID
   * @param appId ����ȡ[[Event]]��appid
   * @param channelId ����ȡ[[Event]]��channelid, ��ѡ
   */
  def futureGet(
      eventId: String,
      appId: Int,
      channelId: Option[Int]
    )(implicit ec: ExecutionContext): Future[Option[Event]]

  /**
   * ɾ��һ���ض���[[Event]]�� ������
   *
   * @param eventId [[Event]]��ID
   * @param appId ��ɾ��[[Event]]��appid
   */
  def futureDelete(eventId: String, appId: Int)(implicit ec: ExecutionContext):
    Future[Boolean] = futureDelete(eventId, appId, None)

  /**
   * ɾ��һ���ض���[[Event]]�� ������
   *
   * @param eventId [[Event]]��ID
   * @param appId ��ɾ��[[Event]]��appid
   * @param channelId ��ɾ��[[Event]]��channelid, ��ѡ
   */
  def futureDelete(
      eventId: String,
      appId: Int,
      channelId: Option[Int]
    )(implicit ec: ExecutionContext): Future[Boolean]

  /**
   * �����ݿ��ж�ȡ������һ��[[Event]]�ļ��ϵĵ�����Future
   *
   * @param appId ���ش�appId��events
   * @param channelId ���ش�channel��events ��ѡ
   * @param startTime ������������������events eventTime >= startTime
   * @param untilTime ������������������events eventTime < untilTime
   * @param entityType ��������entityType��events
   * @param entityId ��������entityId��events
   * @param eventNames ����ƥ��eventNames�е��κ�һ��name��events
   * @param targetEntityType ��������targetEntityType��events��
   *   - None�� û��targetEntityTypeԼ��
   *   - Some(None)�� ���eventû��targetEntityType
   *   - Some(Some(x)): targetEntityTypeƥ��x
   * @param targetEntityID ��������targetEntityId��events��
   *   - None�� û��targetEntityIdԼ��
   *   - Some(None)�� ���eventû��targetEntityId
   *   - Some(Some(x)): targetEntityIdƥ��x
   * @param limit events��Ŀ�����ޣ�None��Some(-1)��ʾû�����ޣ� ��ȡȫ��
   * @param reversed ����
   *   - Ĭ�� �����ǰ
   *   - Some(true) ������ǰ
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

  /** ��������events��ʵ������Խ��оۺ�:
    * \$set, \$unset, \$delete events.
    * ����ʵ��ID��ʵ�����Ե�Map��Future.
    *
    * @param appId �����appID��events�оۺ�
    * @param channelId �����channelID��events�оۺ� Ĭ��ΪNone
    * @param entityType ���ۺ�ʵ���ʵ������
    * @param startTime eventsԼ�� eventTime >= startTime
    * @param untilTime eventsԼ�� eventTime < untilTime
    * @param required ֻ����������required��ȫ�����Ե�Entity��Ӧ��event
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

  /** ��������events��ʵ������Խ��оۺ�:��entityType + entityId��
    * \$set, \$unset, \$delete events.
    * ����ʵ��ID��ʵ�����Ե�Map��Future.
    *
    * @param appId �����appID��events�оۺ�
    * @param channelId �����channelID��events�оۺ� Ĭ��ΪNone
    * @param entityType ���ۺ�ʵ���ʵ������
    * @param startTime eventsԼ�� eventTime >= startTime
    * @param untilTime eventsԼ�� eventTime < untilTime
    * @param entityId ʵ������id
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

  //�����汾
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
   * ��ȡ���ݿ⣬����events������
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
