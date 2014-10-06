<?php
namespace Packaged\Mappers;

use cassandra\AuthenticationException;
use cassandra\AuthorizationException;
use cassandra\CassandraClient;
use cassandra\Column;
use cassandra\Compression;
use cassandra\ConsistencyLevel;
use cassandra\CqlPreparedResult;
use cassandra\CqlResult;
use cassandra\CqlResultType;
use cassandra\CqlRow;
use cassandra\InvalidRequestException;
use cassandra\NotFoundException;
use cassandra\SchemaDisagreementException;
use cassandra\TimedOutException;
use cassandra\UnavailableException;
use Packaged\Mappers\Exceptions\CassandraException;
use Thrift\Exception\TApplicationException;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TFramedTransport;
use Thrift\Transport\TSocketPool;

class ThriftConnection implements IConnection
{
  protected $_hosts;
  protected $_deadHosts = [];
  protected $_port;
  protected $_persistConnection = false;
  protected $_recieveTimeout = 1000;
  protected $_sendTimeout = 1000;
  protected $_connectTimeout = 200;

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

  protected $_keyspace = null;

  /**
   * @var ThriftCQLPreparedStatement[]
   */
  protected $_stmtCache = [];

  protected static $_hostRetryAttempts = 2;
  protected $_thisHostAttemptsLeft;

  protected static $_allHostsAttempts = 1;
  protected $_allHostAttemptsLeft;

  public static function newConnection($config)
  {
    if(isset($config['hosts']))
    {
      $hosts = (array)$config['hosts'];
    }
    else if(isset($config['host']))
    {
      $hosts = (array)$config['host'];
    }
    else
    {
      $hosts = ['localhost'];
    }

    if(isset($config['port']))
    {
      $instance = new static($hosts, (int)$config['port'] ?: 9160);
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

    if(isset($config['connect_timeout']))
    {
      $instance->setConnectTimeout($config['connect_timeout']);
    }

    if(isset($config['send_timeout']))
    {
      $instance->setSendTimeout($config['send_timeout']);
    }

    if(isset($config['receive_timeout']))
    {
      $instance->setReceiveTimeout($config['receive_timeout']);
    }

    return $instance;
  }

  public function __construct(array $hosts = ['localhost'], $port = 9160)
  {
    $this->_hosts = $hosts;
    $this->_port  = $port;

    $this->_thisHostAttemptsLeft = static::$_hostRetryAttempts;
    $this->_allHostAttemptsLeft  = static::$_allHostsAttempts;
  }

  public function setConnectTimeout($timeout)
  {
    $this->_connectTimeout = $timeout;
    return $this;
  }

  public function setReceiveTimeout($timeout)
  {
    $this->_recieveTimeout = $timeout;
    if($this->socket() instanceof TSocketPool)
    {
      $this->socket()->setRecvTimeout($timeout);
    }
    return $this;
  }

  public function setSendTimeout($timeout)
  {
    $this->_sendTimeout = $timeout;
    if($this->socket() instanceof TSocketPool)
    {
      $this->socket()->setSendTimeout($timeout);
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

  public function getAvailableHosts()
  {
    return array_diff($this->_hosts, $this->_deadHosts);
  }

  public function addHost($host, $port = null)
  {
    $this->_hosts[] = $host;
    if($port === null)
    {
      $port = $this->_port;
    }
    if($this->socket() instanceof TSocketPool)
    {
      $this->socket()->addServer($host, $port);
    }
    return $this;
  }

  public function client()
  {
    if($this->_client === null)
    {
      $this->socket()->setDebug(true);
      $this->socket()->setSendTimeout($this->_connectTimeout);

      // TODO: Switch back to TBinaryProtocolAccelerated once HHVM is fixed
      $this->_protocol = new TBinaryProtocol($this->transport());
      $this->_client   = new CassandraClient($this->_protocol);

      $this->transport()->open();
      $this->_connected = true;

      $this->socket()->setRecvTimeout($this->_recieveTimeout);
      $this->socket()->setSendTimeout($this->_sendTimeout);

      if($this->_keyspace)
      {
        $this->_client->set_keyspace($this->_keyspace);
      }
    }
    return $this->_client;
  }

  public function isConnected()
  {
    return (bool)$this->_connected;
  }

  public function disconnect()
  {
    $this->_clearStmtCache();
    $this->_client = null;
    $this->transport()->close();
    $this->_transport = null;
    $this->_protocol  = null;
    $this->_socket    = null;
    $this->_connected = false;
  }

  protected function _dropHost($host = null)
  {
    if($host == null)
    {
      $host = $this->socket()->getHost();
    }
    if($host)
    {
      $this->disconnect();
      $this->_deadHosts[] = $host;
      if($this->_allHostAttemptsLeft > 0)
      {
        $this->_thisHostAttemptsLeft = static::$_hostRetryAttempts;
        if(!$this->getAvailableHosts())
        {
          $this->_allHostAttemptsLeft--;
          $this->_deadHosts = [];
        }
      }
    }
  }

  public function setKeyspace($keyspace)
  {
    try
    {
      $this->_keyspace = $keyspace;
      if($this->_client)
      {
        $this->client()->set_keyspace($keyspace);
      }
    }
    catch(InvalidRequestException $e)
    {
      throw new CassandraException(
        "The keyspace `$keyspace` could not be found", 404
      );
    }
    return $this;
  }

  public function getKeyspace()
  {
    return $this->_keyspace;
  }

  /**
   * @return TSocketPool
   * @throws CassandraException
   */
  public function socket()
  {
    if(!$this->_socket)
    {
      $hosts = $this->getAvailableHosts();
      if(!$hosts)
      {
        throw new CassandraException(
          'TSocketPool: All hosts in pool are down. ('
          . implode(',', $this->_hosts) . ')'
        );
      }

      $this->_socket = $this->_newSocket($hosts);
    }
    return $this->_socket;
  }

  protected function _newSocket($hosts)
  {
    return new TSocketPool(
      $hosts, $this->_port, $this->_persistConnection
    );
  }

  public function transport()
  {
    if(!$this->_transport)
    {
      $this->_transport = new TFramedTransport($this->socket());
    }
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

  /**
   * @param string $query
   * @param int    $compression
   * @param bool   $allowStmtCache
   *
   * @return ThriftCQLPreparedStatement
   * @throws Exceptions\CassandraException
   */
  public function prepare(
    $query, $compression = Compression::NONE, $allowStmtCache = true
  )
  {
    if($compression === null)
    {
      $compression = Compression::NONE;
    }
    while(true)
    {
      try
      {
        $client   = $this->client();
        $cacheKey = md5($query . '@' . $this->socket()->getHost());
        if($allowStmtCache && (!empty($this->_stmtCache[$cacheKey])))
        {
          $stmt = $this->_stmtCache[$cacheKey];
        }
        else
        {
          $prep = $client->prepare_cql3_query(
            $query,
            $compression
          );
          /**
           * @var $prep CqlPreparedResult
           */
          $stmt = new ThriftCQLPreparedStatement(
            $this,
            $prep,
            $query,
            $compression
          );
          if($allowStmtCache)
          {
            $this->_stmtCache[$cacheKey] = $stmt;
          }

          $this->_thisHostAttemptsLeft = static::$_hostRetryAttempts;
        }
        return $stmt;
      }
      catch(\Exception $e)
      {
        if(static::_isPermanentFailure($e))
        {
          throw $this->formException($e);
        }
        if($this->_thisHostAttemptsLeft <= 0 && $this->_allHostAttemptsLeft <= 0)
        {
          throw $this->formException($e);
        }
        if(--$this->_thisHostAttemptsLeft <= 0)
        {
          $this->_dropHost();
        }
      }
    }
    throw new CassandraException('Unable to prepare statement.');
  }

  /**
   * @param IPreparedStatement $statement
   * @param array              $parameters
   * @param int                $consistency
   *
   * @return array|mixed
   * @throws Exceptions\CassandraException
   * @throws \Exception
   */
  public function execute(
    IPreparedStatement $statement, array $parameters = [],
    $consistency = ConsistencyLevel::QUORUM
  )
  {
    if(!($statement instanceof ThriftCQLPreparedStatement))
    {
      throw new CassandraException(
        'Statement not an instance of ThriftCQLPreparedStatement'
      );
    }

    //$retries = static::$_queryRetryCount;
    while(true)
    {
      try
      {
        if(!$statement->isHost($this->socket()->getHost()))
        {
          $statement = $this->prepare(
            $statement->getQuery(),
            $statement->getCompression()
          );
        }

        $result = $this->client()->execute_prepared_cql3_query(
          $statement->getQueryId(),
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

        $returnRows = [];
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

          $returnRows[] = $resultRow;
        }

        $this->_thisHostAttemptsLeft = static::$_hostRetryAttempts;
        return $returnRows;
      }
      catch(\Exception $e)
      {
        if(static::_isPermanentFailure($e))
        {
          throw $this->formException($e);
        }
        if($this->_thisHostAttemptsLeft <= 0 && $this->_allHostAttemptsLeft <= 0)
        {
          throw $this->formException($e);
        }
        if(--$this->_thisHostAttemptsLeft <= 0)
        {
          $this->_dropHost();
        }
      }
    }
    throw new CassandraException('Unable to execute Statement.');
  }

  protected static function _isPermanentFailure(\Exception $e)
  {
    if($e instanceof InvalidRequestException)
    {
      return true;
    }
    return false;
  }

  private function _clearStmtCache()
  {
    $this->_stmtCache = [];
  }

  public function formException(\Exception $e)
  {
    try
    {
      throw $e;
    }
    catch(NotFoundException $e)
    {
      return new CassandraException(
        "A specific column was requested that does not exist.", 404, $e
      );
    }
    catch(InvalidRequestException $e)
    {
      return new CassandraException(
        "Invalid request could mean keyspace or column family does not exist," .
        " required parameters are missing, or a parameter is malformed. " .
        "why contains an associated error message.", 400, $e
      );
    }
    catch(UnavailableException $e)
    {
      return new CassandraException(
        "Not all the replicas required could be created and/or read", 503, $e
      );
    }
    catch(TimedOutException $e)
    {
      return new CassandraException(
        "The node responsible for the write or read did not respond during" .
        " the rpc interval specified in your configuration (default 10s)." .
        " This can happen if the request is too large, the node is" .
        " oversaturated with requests, or the node is down but the failure" .
        " detector has not yet realized it (usually this takes < 30s).",
        408, $e
      );
    }
    catch(TApplicationException $e)
    {
      return new CassandraException(
        "Internal server error or invalid Thrift method (possible if " .
        "you are using an older version of a Thrift client with a " .
        "newer build of the Cassandra server).", 500, $e
      );
    }
    catch(AuthenticationException $e)
    {
      return new CassandraException(
        "Invalid authentication request " .
        "(user does not exist or credentials invalid)", 401, $e
      );
    }
    catch(AuthorizationException $e)
    {
      return new CassandraException(
        "Invalid authorization request (user does not have access to keyspace)",
        403, $e
      );
    }
    catch(SchemaDisagreementException $e)
    {
      return new CassandraException(
        "Schemas are not in agreement across all nodes", 500, $e
      );
    }
    catch(\Exception $e)
    {
      return new CassandraException($e->getMessage(), $e->getCode(), $e);
    }
  }
}
