<?php

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class BaseMapperTest extends PHPUnit_Framework_TestCase
{
  public function setUp()
  {
    require_once __DIR__ . '/User.php';

    $db = \Doctrine\ORM\EntityManager::create(
      [
        'driver'   => 'pdo_mysql',
        'host'     => 'localhost',
        'dbname'   => 'cubex',
        'user'     => 'root',
        'password' => ''
      ],
      \Doctrine\ORM\Tools\Setup::createAnnotationMetadataConfiguration(
        [dirname(__DIR__)],
        true
      )
    );
    $db->getConnection()->getConfiguration()->setSQLLogger(
    //  new \Doctrine\DBAL\Logging\EchoSQLLogger()
    );

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

    $tool    = new \Doctrine\ORM\Tools\SchemaTool($db);
    $classes = [$db->getClassMetadata('User')];
    $tool->dropSchema($classes);
    $tool->createSchema($classes);
  }

  /**
   * @dataProvider connectionsData
   * @param $connectionName
   * @param $expectedClass
   * @param $expectedException
   */
  public function testConnections($connectionName, $expectedClass, $expectedException)
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

  public function compareObjects($obj1, $obj2)
  {
    $this->assertEquals((array)$obj1, (array)$obj2);
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

    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
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
    $this->compareObjects(User::loadWhere([]), [$user, $newUser]);

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
}
