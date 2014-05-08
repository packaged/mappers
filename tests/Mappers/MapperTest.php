<?php

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class MapperTest extends PHPUnit_Framework_TestCase
{
  public function setUp()
  {
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

  public function testConnections()
  {
    $resolver = \Packaged\Mappers\BaseMapper::getConnectionResolver();
    $this->setExpectedException('\Packaged\Mappers\Exceptions\MapperException');
    $resolver->getConnection('DOES NOT EXIST');
    $this->setExpectedException(null);
    $resolver->getConnection('db');
  }

  public function compareObjects($obj1, $obj2)
  {
    foreach($obj1 as $k => $v)
    {
      $this->assertEquals($obj1->$k, $obj2->$k);
    }
    foreach($obj2 as $k => $v)
    {
      $this->assertEquals($obj2->$k, $obj1->$k);
    }
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
    $this->setExpectedException(
      '\Packaged\Mappers\Exceptions\InvalidLoadException'
    );
    new User('invalid call');
    User::load(null);
    $this->setExpectedException(null);

    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();

    $loaded = User::load($user->id());
    $this->assertEquals($user->id(), $loaded->id());
    $this->compareObjects($user, $loaded);

    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $this->assertTrue($user->exists());

    $loadedUser = User::load($user->id());
    $this->assertTrue($loadedUser->exists());
    $this->compareObjects($user, $loadedUser);
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
