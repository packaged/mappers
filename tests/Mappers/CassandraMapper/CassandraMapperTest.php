<?php

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class CassandraMapperTest extends PHPUnit_Framework_TestCase
{
  public function setUp()
  {
    require_once __DIR__ . '/User.php';
    require_once __DIR__ . '/Person.php';

    $cassDb = new \Packaged\Mappers\ThriftConnection(['localhost']);
    $cassDb->setKeyspace('Cubex');

    $resolver = new \Packaged\Mappers\ConnectionResolver();
    $resolver->addConnection('cassdb', $cassDb);

    \Packaged\Mappers\BaseMapper::setConnectionResolver($resolver);
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

    $newUser = $user->saveAsNew(uniqid('newtest'));
    var_dump(CassUser::loadWhere([]));

    $this->compareObjects(CassUser::loadWhere([]), [$user, $newUser]);

    $user->name = 'name' . rand() . time();
    $user->save();
    $this->compareObjects(
      CassUser::loadWhere(['name' => $user->name]),
      [$user]
    );
  }

  public function testReload()
  {
    $user              = new CassUser();
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
    $user->name        = 'testDelete';
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->compareObjects($user, CassUser::load($id));

    $user->delete();

    $deleted = CassUser::load($id);
    $this->assertFalse($deleted->exists());
    $this->compareObjects($deleted, new CassUser());
  }

  public function testValidationFailure()
  {
    $person = new CassPerson();
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
    $user              = new CassUser();
    $user->name        = 'test ' . rand();
    $user->description = '';
    $user->save();

    $testUser = CassUser::load($user->id());
    $this->compareObjects($user, $testUser);

    $user->increment('countField', 1);

    $testUser = User::load($user->id());

    $this->assertEquals(1, $user->countField);
    $this->assertEquals($user->countField, $testUser->countField);

    $user->decrement('countField', 50);
    $this->assertEquals(-49, $user->countField);

    $user->increment('countField', 100);
    $user->reload();
    $this->assertEquals(51, $user->countField);
  }
}
