<?php
use Doctrine\ORM\Configuration;
use Doctrine\ORM\Tools\Setup;
use Packaged\Mappers\Mapping\AutoMappingDriver;
use Packaged\Mappers\Mapping\ChainedDriver;

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class DoctrineMapperTest extends PHPUnit_Framework_TestCase
{
  public static function setUpBeforeClass()
  {
    require_once __DIR__ . '/User.php';
    require_once __DIR__ . '/Person.php';

    $conf = Setup::createConfiguration(true);
    $conf->setMetadataDriverImpl(
      new ChainedDriver(
        [
          (new Configuration())->newDefaultAnnotationDriver(),
          new AutoMappingDriver()
        ],
        [__DIR__ . '/Mappers']
      )
    );

    $db = \Doctrine\ORM\EntityManager::create(
      [
        'driver'   => 'pdo_mysql',
        'host'     => 'localhost',
        'dbname'   => 'cubex',
        'user'     => 'root',
        'password' => ''
      ],
      $conf
    );
    /*$db->getConnection()->getConfiguration()->setSQLLogger(
      new \Doctrine\DBAL\Logging\EchoSQLLogger()
    );*/

    $sqlite = \Doctrine\ORM\EntityManager::create(
      [
        'driver' => 'pdo_sqlite',
        'path'   => dirname(dirname(__DIR__)) . '/data/db.sqlite',
      ],
      \Doctrine\ORM\Tools\Setup::createAnnotationMetadataConfiguration(
        [dirname(__DIR__)],
        true
      )
    );

    $resolver = new \Packaged\Mappers\ConnectionResolver();
    $resolver->addConnection('db', $db);
    $resolver->addConnection('sqlite', $sqlite);

    \Packaged\Mappers\BaseMapper::setConnectionResolver($resolver);

    User::createTable();
    Person::createTable();
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
      ['db', 'Doctrine\ORM\EntityManager', null],
      ['sqlite', 'Doctrine\ORM\EntityManager', null],
      ['DOES NOT EXIST', null, '\Packaged\Mappers\Exceptions\MapperException']
    ];
  }

  public function testNew()
  {
    $user              = new User();
    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->assertNotEmpty($id);
    $this->compareObjects($user, User::load($id));
    $user->name        = rand();
    $user->description = rand();

    $user = $user->saveAsNew();
    $this->assertNotSame($id, $user->id());
    $this->compareObjects($user, User::load($user->id()));
  }

  public function compareObjects($obj1, $obj2)
  {
    $this->assertEquals((array)$obj1, (array)$obj2);
  }

  public function testLoad()
  {
    try
    {
      new User('invalid call');
      $this->fail('Expected Exception not thrown');
    }
    catch(\Packaged\Mappers\Exceptions\InvalidLoadException $e)
    {
    }
    try
    {
      User::load(null);
      $this->fail('Expected Exception not thrown');
    }
    catch(\Packaged\Mappers\Exceptions\InvalidLoadException $e)
    {
    }

    $uniqueDescription = uniqid('usertesting');

    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = $uniqueDescription;
    $user->save();
    $this->assertTrue($user->exists());

    $loaded = User::load($user->id());
    $this->assertEquals($user->id(), $loaded->id());
    $this->compareObjects($user, $loaded);

    $loadedUser = User::load($user->id());
    $this->assertTrue($loadedUser->exists());
    $this->compareObjects($user, $loadedUser);

    $newUser = $user->saveAsNew();
    $user->getEntityManager()->detach($user);
    $newUser->getEntityManager()->detach($newUser);
    $this->compareObjects(User::loadWhere(['description'=>$uniqueDescription]), [$user, $newUser]);

    $user->name = 'name' . rand() . time();
    $user->save();
    $this->compareObjects(User::loadWhere(['name' => $user->name]), [$user]);
  }

  public function testReload()
  {
    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();

    $user->name        = 'reload' . rand();
    $user->description = 'reload' . rand();
    $user->reload();

    $this->compareObjects($user, User::load($user->id()));
  }

  public function testDelete()
  {
    $user              = new User();
    $user->name        = 'testDelete';
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->compareObjects($user, User::load($id));

    $user->delete();

    $deleted = User::load($id);
    $this->assertFalse($deleted->exists());
    $this->compareObjects($deleted, new User());
  }

  public function testValidationFailure()
  {
    $person = new Person();
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
    $person              = new Person();
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
    $person            = new Person();
    $person->testField = 'test data';
    $this->setExpectedException(
      '\Respect\Validation\Exceptions\AllOfException',
      'These rules must pass for "test data"'
    );
    $person->validateField('testField');
  }

  public function testIncrementDecrement()
  {
    $user              = new User();
    $user->name        = 'test ' . rand();
    $user->description = '';
    $user->save();

    $testUser = User::load($user->id());
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
