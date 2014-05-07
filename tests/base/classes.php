<?php

/**
 * @Entity @Table(name="users")
 **/
class User extends \Packaged\Mappers\BaseMapper
{
  /**
   * @Id @Column(type="integer") @GeneratedValue
   **/
  public $id;

  /**
   * @Column(type="string")
   **/
  public $name;

  /**
   * @Column(type="string")
   **/
  public $description;
}

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
//$resolver->addConnection('cql', $cql);
$resolver->addConnection('db', $db);
$resolver->addConnection('sqlite', $sqlite);

\Packaged\Mappers\BaseMapper::setConnectionResolver($resolver);

$tool = new \Doctrine\ORM\Tools\SchemaTool($db);
$tool->createSchema([$db->getClassMetadata('User')]);