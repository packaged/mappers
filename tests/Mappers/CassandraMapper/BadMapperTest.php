<?php
namespace Mappers\CassandraMapper;

use cassandra\ConsistencyLevel;
use Packaged\Mappers\CassandraMapper;
use Packaged\Mappers\ConnectionResolver;
use Packaged\Mappers\IPreparedStatement;
use Packaged\Mappers\ThriftConnection;
use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Transport\TSocketPool;

class BadMapperTest extends \PHPUnit_Framework_TestCase
{
  /**
   * @expectedException     \Packaged\Mappers\Exceptions\CassandraException
   * @expectedExceptionMessage TSocketPool: All hosts in pool are down.
   */
  public function testNoConnection()
  {
    $mapper       = new BadCassandraMapper();
    $mapper->test = 'test';
    $mapper->save();
  }

  /**
   * @expectedException \Thrift\Exception\TException
   * @expectedExceptionMessage TSocketPool: All hosts in pool are down. (localhost)
   */
  public function testMockConnection()
  {
    $mapper       = new MockCassandraMapper();
    $mapper->test = 'test';
    $mapper->save();
  }
}

class MockSocket extends TSocketPool
{
  public function read($len)
  {
    throw new TTransportException('TSocketPool: my mock connection is broken');
  }

  public function write($buf)
  {
    throw new TTransportException('TSocketPool: my mock connection is broken');
  }
}

class MockThriftConnection extends ThriftConnection
{
  protected function _newSocket($hosts)
  {
    return new MockSocket(
      $hosts, $this->_port, $this->_persistConnection
    );
  }
}

class MockCassandraMapper extends CassandraMapper
{
  public $test;

  public static function getServiceName()
  {
    return 'db';
  }

  public static function getConnectionResolver()
  {
    $db = new MockThriftConnection(['localhost']);

    $resolver = new ConnectionResolver();
    $resolver->addConnection('db', $db);

    return $resolver;
  }
}

class BadThriftConnection extends ThriftConnection
{
  public function execute(
    IPreparedStatement $statement, array $parameters = [],
    $consistency = ConsistencyLevel::QUORUM
  )
  {
    throw new \Exception('some error');
  }

  public function client()
  {
    throw new TException('TSocketPool: All hosts in pool are down.');
  }
}

class BadCassandraMapper extends CassandraMapper
{
  public $test;

  public static function getServiceName()
  {
    return 'db';
  }

  public static function getConnectionResolver()
  {
    $db = new BadThriftConnection();

    $resolver = new ConnectionResolver();
    $resolver->addConnection('db', $db);

    return $resolver;
  }
}
