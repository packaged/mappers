<?php
use Doctrine\ORM\Configuration;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Tools\SchemaTool;
use Doctrine\ORM\Tools\Setup;
use Packaged\Mappers\DoctrineMapper;
use Packaged\Mappers\ConnectionResolver;
use Packaged\Mappers\Mapping\AutoMappingDriver;
use Packaged\Mappers\Mapping\ChainedDriver;

/**
 * @author  Richard.Gooding
 */
class AutoTimestampTest extends PHPUnit_Framework_TestCase
{
  /**
   * @var ConnectionResolver
   */
  private $_resolver;

  public function setUp()
  {
    require_once __DIR__ . '/Mappers/DifferentFieldsMapper.php';
    require_once __DIR__ . '/Mappers/NonTimestampMapper.php';
    require_once __DIR__ . '/Mappers/TimestampMapper.php';

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

    $db = EntityManager::create(
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

    $this->_resolver = new ConnectionResolver();
    $this->_resolver->addConnection('db', $db);

    DoctrineMapper::setConnectionResolver($this->_resolver);

    $tool    = new SchemaTool($db);
    $classes = [
      $db->getClassMetadata('DifferentFieldsMapper'),
      $db->getClassMetadata('NonTimestampMapper'),
      $db->getClassMetadata('TimestampMapper')
    ];
    $tool->dropSchema($classes);
    $tool->createSchema($classes);
  }

  public function testWithTimestamp()
  {
    $now              = time();
    $mapper           = new TimestampMapper();
    $mapper->someData = 'test data';
    $mapper->save();

    $newMapper = TimestampMapper::load($mapper->id());

    $this->assertInstanceOf('DateTime', $newMapper->createdAt);
    $this->assertInstanceOf('DateTime', $newMapper->updatedAt);
    $this->assertEquals($now, $newMapper->createdAt->getTimestamp(), '', 10);
    $this->assertEquals($now, $newMapper->updatedAt->getTimestamp(), '', 10);
  }

  public function testWithoutTimestamp()
  {
    $mapper           = new NonTimestampMapper();
    $mapper->someData = 'test data';
    $mapper->save();

    $newMapper = NonTimestampMapper::load($mapper->id());
    print_r($newMapper);
  }
  /*
    public function testDifferentFields()
    {

    }
  */

  /**
   * @dataProvider fieldMappingsData
   *
   * @param string $className
   * @param array  $expectedFieldMappings
   *
   * @throws Packaged\Mappers\Exceptions\MapperException
   */
  public function testFieldMappings($className, $expectedFieldMappings)
  {
    /**
     * @var EntityManager $conn
     */
    $conn                = $this->_resolver->getConnection('db');
    $actualFieldMappings = $conn->getClassMetadata($className)->fieldMappings;

    ksort($expectedFieldMappings);
    ksort($actualFieldMappings);

    echo "\n-------------------------------------\n" . $className . "\n";
    echo "EXPECTED:\n";
    print_r($expectedFieldMappings);
    echo "ACTUAL:\n";
    print_r($actualFieldMappings);

    $this->assertEquals($expectedFieldMappings, $actualFieldMappings);
  }

  public function fieldMappingsData()
  {
    $baseFields = [
      'id'       => [
        'fieldName'     => 'id',
        'columnName'    => 'id',
        'type'          => 'integer',
        'id'            => true,
        'autoincrement' => true,
        'unsigned'      => true,
      ],
      'someData' => [
        'fieldName'  => 'someData',
        'columnName' => 'some_data',
        'type'       => 'string',
      ],
    ];

    return [
      ['NonTimestampMapper', $baseFields],
      [
        'TimestampMapper',
        array_merge(
          [
            'createdAt' => [
              'fieldName'  => 'createdAt',
              'columnName' => 'created_at',
              'type'       => 'datetime',
              //'declared' => 'Packaged\Mappers\BaseMapper',
            ],
            'updatedAt' => [
              'fieldName'  => 'updatedAt',
              'columnName' => 'updated_at',
              'type'       => 'datetime',
              //'declared' => 'Packaged\Mappers\BaseMapper',
            ],
          ],
          $baseFields
        )
      ],
      [
        'DifferentFieldsMapper',
        array_merge(
          $baseFields,
          [
            'createdAt' => [
              'fieldName'  => 'createdAt',
              'columnName' => 'other_created_at',
              'type'       => 'datetime',
              //'declared' => 'Packaged\Mappers\BaseMapper',
            ],
            'updatedAt' => [
              'fieldName'  => 'updatedAt',
              'columnName' => 'other_updated_at',
              'type'       => 'datetime',
              //'declared' => 'Packaged\Mappers\BaseMapper',
            ],
          ]
        )
      ]
    ];
  }
}
