<?php

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class CassandraMapperTest extends PHPUnit_Framework_TestCase
{
  public static function setUpBeforeClass()
  {
    require_once __DIR__ . '/User.php';
    require_once __DIR__ . '/Person.php';
    require_once __DIR__ . '/CounterTest.php';
    require_once __DIR__ . '/KeyedUser.php';

    $cassDb = new \Packaged\Mappers\ThriftConnection(['localhost']);
    $stmt   = $cassDb->prepare(
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

    $resolver = new \Packaged\Mappers\ConnectionResolver();
    $resolver->addConnection('cassdb', $cassDb);

    \Packaged\Mappers\BaseMapper::setConnectionResolver($resolver);

    CassUser::createTable();
    CassPerson::createTable();
    CounterTest::createTable();
  }

  public static function tearDownAfterClass()
  {
    $resolver = \Packaged\Mappers\BaseMapper::getConnectionResolver();
    $cassDb   = $resolver->getConnection('cassdb');

    $stmt = $cassDb->prepare('DROP KEYSPACE "test_cassandra_mapper"');
    $stmt->execute();
  }

  /**
   * @dataProvider connectionsData
   *
   * @param $connectionName
   * @param $expectedClass
   * @param $expectedException
   */
  public function testConnections(
    $connectionName, $expectedClass, $expectedException
  )
  {
    $resolver = \Packaged\Mappers\BaseMapper::getConnectionResolver();
    $this->setExpectedException($expectedException);
    $this->assertInstanceOf(
      $expectedClass,
      $resolver->getConnection($connectionName)
    );
  }

  public function connectionsData()
  {
    return [
      ['cassdb', 'Packaged\Mappers\ThriftConnection', null],
      ['DOES NOT EXIST', null, '\Packaged\Mappers\Exceptions\MapperException']
    ];
  }

  public function testNew()
  {
    $testId            = uniqid('testing');
    $user              = new CassUser();
    $user->id          = $testId;
    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->assertNotEmpty($id);

    $this->compareObjectsSimilar($user, CassUser::load($id));
    $user->name        = rand();
    $user->description = rand();

    $user = $user->saveAsNew(uniqid('testingnew'));
    $this->assertNotSame($id, $user->id());
    $this->compareObjects($user, CassUser::load($user->id()));
  }

  public function compareObjects($obj1, $obj2)
  {
    $this->assertEquals((array)$obj1, (array)$obj2);
  }

  public function compareObjectsSimilar($obj1, $obj2)
  {
    $obj1 = (array)$obj1;
    $obj2 = (array)$obj2;
    array_walk(
      $obj1,
      function ($a) { return is_scalar($a) ? (string)$a : $a; }
    );
    array_walk(
      $obj2,
      function ($a) { return is_scalar($a) ? (string)$a : $a; }
    );
    $this->assertEquals($obj1, $obj2);
  }

  public function testLoad()
  {
    try
    {
      new CassUser('invalid call');
      $this->fail('Expected Exception not thrown');
    }
    catch(\Packaged\Mappers\Exceptions\InvalidLoadException $e)
    {
    }
    try
    {
      CassUser::load(null);
      $this->fail('Expected Exception not thrown');
    }
    catch(\Packaged\Mappers\Exceptions\InvalidLoadException $e)
    {
    }

    $user              = new CassUser();
    $user->id          = uniqid('testing');
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();
    $this->assertTrue($user->exists());

    $loaded = CassUser::load($user->id());
    $this->assertEquals($user->id(), $loaded->id());
    $this->compareObjects($user, $loaded);

    $loadedUser = CassUser::load($user->id());
    $this->assertTrue($loadedUser->exists());
    $this->compareObjects($user, $loadedUser);
    /*$newUser = $user->saveAsNew(uniqid('newtest'));

    $this->compareObjects(
      CassUser::loadWhere(['KEY = \'' . $user->id() . '\'']),
      [$user]
    );

    $this->compareObjects(CassUser::loadWhere([]), [$user, $newUser]);

    $user->name = 'name' . rand() . time();
    $user->save();
    $this->compareObjects(
      CassUser::loadWhere(['name' => $user->name]),
      [$user]
    );*/
  }

  public function testReload()
  {
    $user              = new CassUser();
    $user->id          = uniqid('testing');
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();

    $user->name        = 'reload' . rand();
    $user->description = 'reload' . rand();
    $user->reload();

    $this->compareObjects($user, CassUser::load($user->id()));
  }

  public function testDelete()
  {
    $user              = new CassUser();
    $user->id          = uniqid('testing');
    $user->name        = 'testDelete';
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->compareObjects($user, CassUser::load($id));

    $user->delete();

    try
    {
      CassUser::load($id);
      $this->fail('Expected Exception not thrown');
    }
    catch(\Packaged\Mappers\Exceptions\InvalidLoadException $e)
    {
    }
    $deleted = CassUser::loadOrNew($id);
    $this->assertFalse($deleted->exists());
  }

  public function testValidationFailure()
  {
    $person     = new CassPerson();
    $person->id = uniqid('persontest');
    // Must be between 2 and 32 characters
    $person->name = '';
    // no validation
    $person->description = 'test description';
    // must end with "test"
    $person->testField = 'some data';

    $this->assertFalse($person->validate(false));
    $this->assertFalse($person->validateField('name', false));
    $this->assertFalse($person->validateField('testField', false));

    $this->setExpectedException(
      '\Respect\Validation\Exceptions\AllOfException',
      'These rules must pass for ""'
    );
    $person->save();
  }

  public function testValidationPass()
  {
    $person              = new CassPerson();
    $person->hash        = uniqid('persontest');
    $person->name        = 'Test User';
    $person->description = 'Test description';
    $person->testField   = 'test field test';
    $this->assertTrue($person->validate(false));
    $this->assertTrue($person->validateField('name', false));
    $this->assertTrue($person->validateField('testField', false));
    $person->save();
  }

  public function testFieldValidationFailure()
  {
    $person            = new CassPerson();
    $person->testField = 'test data';
    $this->setExpectedException(
      '\Respect\Validation\Exceptions\AllOfException',
      'These rules must pass for "test data"'
    );
    $person->validateField('testField');
  }

  public function testIncrementDecrement()
  {
    $user     = new CounterTest();
    $user->id = uniqid('testing');

    $user->increment('testCounter', 1);
    $testUser = CounterTest::load($user->id());
    $this->assertEquals(1, $user->testCounter);
    $this->assertEquals($user->testCounter, $testUser->testCounter);

    $user->decrement('testCounter', 50);
    $this->assertEquals(-49, $user->testCounter);

    $user->increment('testCounter', 100);
    $user->increment('maxCounterTest', PHP_INT_MAX);
    $user->reload();
    $this->assertEquals(51, $user->testCounter);
    $this->assertEquals(PHP_INT_MAX, $user->maxCounterTest);
  }

  public function testKeys()
  {
    KeyedUser::createTable();

    $keyspace = 'test_cassandra_mapper';
    $cfName   = 'keyed_users';

    $keys = KeyedUser::execute(
      'SELECT column_aliases, key_aliases '
      . 'FROM system.schema_columnfamilies '
      . 'WHERE keyspace_name = ? '
      . 'AND columnfamily_name = ?',
      [$keyspace, $cfName]
    );

    $this->assertEquals(
      [
        [
          'column_aliases' => '["email","username"]',
          'key_aliases'    => '["brandId","userId"]',
        ]
      ],
      $keys,
      "Key columns do not match"
    );

    $cols = KeyedUser::execute(
      'SELECT column_name FROM system.schema_columns '
      . 'WHERE keyspace_name = ? AND columnfamily_name = ?',
      [$keyspace, $cfName]
    );

    $this->assertEquals(
      [
        ['column_name' => 'created_at'],
        ['column_name' => 'display_name'],
        ['column_name' => 'password'],
        ['column_name' => 'updated_at']
      ],
      $cols,
      "Non-key columns do not match"
    );
  }
}
