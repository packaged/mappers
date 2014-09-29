<?php
namespace Mappers\CassandraMapper;

use cassandra\ConsistencyLevel;
use Packaged\Mappers\BaseMapper;
use Packaged\Mappers\CassandraMapper;
use Packaged\Mappers\Exceptions\CassandraException;
use Packaged\Mappers\IPreparedStatement;
use Packaged\Mappers\ThriftConnection;
use Thrift\Exception\TTransportException;
use Thrift\Transport\TSocketPool;

class BadMapperTest extends \PHPUnit_Framework_TestCase
{
  public static function setUpBeforeClass()
  {
    $resolver = BaseMapper::getConnectionResolver();
    $resolver->addConnection(
      'mock',
      MockThriftConnection::newConnection(['hosts' => 'localhost'])
    );
    $resolver->addConnection(
      'bad',
      BadThriftConnection::newConnection(['hosts' => 'localhost'])
    );
  }

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

  public function testMockConnection()
  {
    try
    {
      $mapper       = new MockCassandraMapper();
      $mapper->test = 'test';
      $mapper->save();
    }
    catch(CassandraException $e)
    {
      if(strpos(
          $e->getMessage(),
          'TSocketPool: All hosts in pool are down.'
        ) !== 0
      )
      {
        throw $e;
      }
    }

    $conn = MockCassandraMapper::getConnection();
    $this->assertInstanceOf(
      '\Mappers\CassandraMapper\MockThriftConnection',
      $conn
    );

    $this->assertEquals(0, $conn->getThisHostRetries());
    $this->assertEquals(0, $conn->getAllHostsRetries());
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

  public function getThisHostRetries()
  {
    return $this->_thisHostAttemptsLeft;
  }

  public function getAllHostsRetries()
  {
    return $this->_allHostAttemptsLeft;
  }
}

class MockCassandraMapper extends CassandraMapper
{
  public $test;

  public static function getServiceName()
  {
    return 'mock';
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
}

class BadCassandraMapper extends CassandraMapper
{
  public $test;

  public static function getServiceName()
  {
    return 'bad';
  }
}
