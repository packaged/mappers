<?php
namespace Mappers\CassandraMapper;

use Packaged\Mappers\BaseMapper;
use Packaged\Mappers\CassandraMapper;
use Packaged\Mappers\Exceptions\CassandraException;
use Packaged\Mappers\ThriftConnection;
use Thrift\Exception\TTransportException;
use Thrift\Transport\TSocketPool;

class BadMapperTest extends \PHPUnit_Framework_TestCase
{
  public static function setUpBeforeClass()
  {
    $cassDb = \Packaged\Mappers\ThriftConnection::newConnection(
      ['hosts' => 'localhost']
    );
    $cassDb->setConnectTimeout(1000);
    $stmt = $cassDb->prepare(
      'SELECT * FROM system.schema_keyspaces where keyspace_name = \'test_cassandra_mapper\''
    );
    if(!$cassDb->execute($stmt))
    {
      $stmt = $cassDb->prepare(
        'CREATE KEYSPACE "test_cassandra_mapper" WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\':1};'
      );
      $cassDb->execute($stmt);
    }
    $cassDb->setKeyspace('test_cassandra_mapper');

    $resolver = BaseMapper::getConnectionResolver();
    $resolver->addConnection(
      'mock',
      MockThriftConnection::newConnection(['hosts' => 'localhost'])
        ->setKeyspace('test_cassandra_mapper')
    );
    $resolver->addConnection(
      'bad',
      ThriftConnection::newConnection(['hosts' => []])
    );
  }

  public static function tearDownAfterClass()
  {
    $cassDb = \Packaged\Mappers\ThriftConnection::newConnection(
      ['hosts' => 'localhost']
    );

    $stmt = $cassDb->prepare('DROP KEYSPACE "test_cassandra_mapper"');
    $stmt->execute();
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

class BadCassandraMapper extends CassandraMapper
{
  public $test;

  public static function getServiceName()
  {
    return 'bad';
  }
}
