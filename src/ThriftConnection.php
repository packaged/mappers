<?php
namespace Packaged\Mappers;

use cassandra\CassandraClient;
use cassandra\Column;
use cassandra\Compression;
use cassandra\ConsistencyLevel;
use cassandra\CqlPreparedResult;
use cassandra\CqlResult;
use cassandra\CqlResultType;
use cassandra\CqlRow;
use cassandra\InvalidRequestException;
use Thrift\Exception\TException;
use Thrift\Protocol\TBinaryProtocolAccelerated;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocketPool;

class ThriftConnection
{
  protected $_queryItemId;

  protected $_hosts;
  protected $_port;
  protected $_persistConnection = false;
  protected $_recieveTimeout = 1000;
  protected $_sendTimeout = 1000;
  protected $_connectTimeout = 100;

  protected $_client;
  protected $_socket;
  protected $_protocol;
  /**
   * @var TFramedTransport
   */
  protected $_transport;

  protected $_connected;

  protected $_processingBatch = false;
  protected $_batchMutation = null;

  public static function newConnection($config)
  {
    $hosts = ['localhost'];
    if(isset($config['hosts']))
    {
      $hosts = (array)$config['hosts'];
    }
    else if(isset($config['host']))
    {
      $hosts = (array)$config['host'];
    }

    if(isset($config['port']))
    {
      $instance = new static($hosts, (int)$config['port'] ? : 9160);
    }
    else
    {
      $instance = new static($hosts);
    }

    /** @var $instance self */
    if(isset($config['keyspace']))
    {
      $instance->setKeyspace($config['keyspace']);
    }

    return $instance;
  }

  public function __construct(array $hosts = ['localhost'], $port = 9160)
  {
    $this->_hosts = $hosts;
    $this->_port  = $port;
  }

  public function setConnectTimeout($timeout)
  {
    $this->_connectTimeout = $timeout;
    return $this;
  }

  public function setReceiveTimeout($timeout)
  {
    $this->_recieveTimeout = $timeout;
    if($this->_socket instanceof TSocketPool)
    {
      $this->_socket->setRecvTimeout($timeout);
    }
    return $this;
  }

  public function setSendTimeout($timeout)
  {
    $this->_sendTimeout = $timeout;
    if($this->_socket instanceof TSocketPool)
    {
      $this->_socket->setSendTimeout($timeout);
    }
    return $this;
  }

  public function setPersistent($enabled)
  {
    $this->_persistConnection = (bool)$enabled;
    return $this;
  }

  public function setPort($port = 9160)
  {
    $this->_port = $port;
    return $this;
  }

  public function setHosts(array $hosts)
  {
    $this->_hosts = $hosts;
    return $this;
  }

  public function getHosts()
  {
    return $this->_hosts;
  }

  public function addHost($host, $port = null)
  {
    $this->_hosts[] = $host;
    if($port === null)
    {
      $port = $this->_port;
    }
    if($this->_socket instanceof TSocketPool)
    {
      $this->_socket->addServer($host, $port);
    }
    return $this;
  }

  public function client()
  {
    if($this->_client === null)
    {
      $this->_socket = new TSocketPool(
        $this->_hosts, $this->_port, $this->_persistConnection
      );

      $this->_socket->setDebug(true);
      $this->_socket->setSendTimeout($this->_connectTimeout);
      $this->_socket->setRetryInterval(0);
      $this->_socket->setNumRetries(1);

      $this->_transport = new TFramedTransport($this->_socket);
      $this->_protocol  = new TBinaryProtocolAccelerated($this->_transport);
      $this->_client    = new CassandraClient($this->_protocol);

      try
      {
        $this->_transport->open();
        $this->_connected = true;
      }
      catch(TException $e)
      {
        $this->_connected = false;
      }

      $this->_socket->setRecvTimeout($this->_recieveTimeout);
      $this->_socket->setSendTimeout($this->_sendTimeout);
    }
    return $this->_client;
  }

  public function isConnected()
  {
    return (bool)$this->_connected;
  }

  public function disconnect()
  {
    $this->_client = null;
    $this->_transport->close();
    $this->_transport = null;
    $this->_protocol  = null;
    $this->_connected = false;
  }

  public function setKeyspace($keyspace)
  {
    try
    {
      $this->client()->set_keyspace($keyspace);
    }
    catch(InvalidRequestException $e)
    {
      throw new \Exception("The keyspace `$keyspace` could not be found", 404);
    }
    return $this;
  }

  public function socket()
  {
    return $this->_socket;
  }

  public function transport()
  {
    return $this->_transport;
  }

  /**
   * @param string      $method
   * @param string|null $default
   * @param array|null  $params
   *
   * @returns string|null
   */
  protected function _describe($method, $default = null, array $params = null)
  {
    $method = 'describe_' . $method;
    if($this->isConnected())
    {
      if($params === null)
      {
        return $this->client()->$method();
      }
      else
      {
        return call_user_func_array([$this->client(), $method], $params);
      }
    }
    return $default;
  }

  public function clusterName()
  {
    return $this->_describe("cluster_name");
  }

  public function schemaVersions()
  {
    return $this->_describe("schema_versions");
  }

  public function keyspaces()
  {
    return $this->_describe("keyspaces");
  }

  public function version()
  {
    return $this->_describe("version");
  }

  public function ring($keyspace)
  {
    return $this->_describe("ring", null, [$keyspace]);
  }

  public function partitioner()
  {
    return $this->_describe("partitioner");
  }

  public function snitch()
  {
    return $this->_describe("snitch");
  }

  public function describeKeyspace($keyspace)
  {
    return $this->_describe("keyspace", null, [$keyspace]);
  }

  public function prepare($query, $compression = Compression::NONE)
  {
    try
    {
      $prep = $this->client()->prepare_cql3_query(
        $query,
        $compression
      );
      /**
       * @var $prep CqlPreparedResult
       */
      $this->_queryItemId = $prep->itemId;
    }
    catch(\Exception $e)
    {
      throw $e;
    }
    return $this;
  }

  public function execute(
    array $parameters = null, $consistency = ConsistencyLevel::QUORUM
  )
  {
    $return = [];
    try
    {
      $result = $this->client()->execute_prepared_cql3_query(
        $this->_queryItemId,
        $parameters,
        $consistency
      );
      /**
       * @var $result CqlResult
       */
      if($result->type == CqlResultType::VOID)
      {
        return true;
      }

      if($result->type == CqlResultType::INT)
      {
        return $result->num;
      }

      foreach($result->rows as $row)
      {
        /**
         * @var $row CqlRow
         */
        $resultRow = [];
        foreach($row->columns as $column)
        {
          /**
           * @var $column Column
           */
          $resultRow[$column->name] = $column->value;
        }

        $return[] = $resultRow;
      }
    }
    catch(\Exception $e)
    {
      throw $e;
    }
    return $return;
  }
}
