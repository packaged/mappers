<?php
/**
 * @author  Richard.Gooding
 */
use Doctrine\ORM\Configuration;
use Doctrine\ORM\Mapping\ClassMetadata;
use Packaged\Mappers\Mapping\AutoMappingDriver;
use Packaged\Mappers\Mapping\ChainedDriver;

/**
 * Class ChainedDriverTest
 */
class ChainedDriverTest extends PHPUnit_Framework_TestCase
{
  private $_loadedMappers = false;

  public function testAllClassNames()
  {
    $driver   = $this->_getDriver();
    $expected = [
      'AnnotatedMapper',
      'PartAnnotatedMapper',
      'UnannotatedMapper'
    ];
    $actual   = $driver->getAllClassNames();
    // Order is not important
    sort($expected);
    sort($actual);
    $this->assertEquals($expected, $actual);
  }

  private function _getDriver()
  {
    return new ChainedDriver(
      [
        (new Configuration())->newDefaultAnnotationDriver(),
        new AutoMappingDriver()
      ],
      [__DIR__ . '/Mappers']
    );
  }

  public function testPaths()
  {
    $driver = new ChainedDriver([]);
    $this->assertEquals([], $driver->getPaths());

    $driver->addPaths(['/tmp/classes', __DIR__ . '/Mappers']);
    $this->assertEquals(
      ['/tmp/classes', __DIR__ . '/Mappers'],
      $driver->getPaths()
    );

    $driver->addPaths([__DIR__ . '/test', '/tmp/otherclasses']);
    $this->assertEquals(
      [
        '/tmp/classes',
        __DIR__ . '/Mappers',
        __DIR__ . '/test',
        '/tmp/otherclasses'
      ],
      $driver->getPaths()
    );

    $driver->addPaths(['/tmp/classes']);
    $this->assertEquals(
      [
        '/tmp/classes',
        __DIR__ . '/Mappers',
        __DIR__ . '/test',
        '/tmp/otherclasses'
      ],
      $driver->getPaths()
    );
  }

  /**
   * @dataProvider transientData
   */
  public function testTransient($className, $isTransient)
  {
    $this->_loadMappers();
    $driver = $this->_getDriver();
    $this->assertEquals($driver->isTransient($className), $isTransient);
  }

  private function _loadMappers()
  {
    if(!$this->_loadedMappers)
    {
      $mappersDir = __DIR__ . '/Mappers';
      $dh         = opendir($mappersDir);
      if(!$dh)
      {
        throw new Exception('Error loading mapper classes');
      }
      while(($file = readdir($dh)))
      {
        if(substr($file, -4) == '.php')
        {
          require_once $mappersDir . '/' . $file;
        }
      }
      closedir($dh);
      $this->_loadedMappers = true;
    }
  }

  public function transientData()
  {
    return [
      ['AnnotatedMapper', false],
      ['PartAnnotatedMapper', false],
      ['UnannotatedMapper', false],
      ['ChainedNotAMapper', true]
    ];
  }

  /**
   * @dataProvider mapperClassesData
   *
   * @param $className
   * @param $expectedFieldMappings
   *
   * @throws Exception
   */
  public function testFieldMappings($className, $expectedFieldMappings)
  {
    $this->_loadMappers();
    $driver = $this->_getDriver();
    $meta   = new ClassMetadata($className);
    $driver->loadMetadataForClass($className, $meta);
    $this->assertEquals($expectedFieldMappings, $meta->fieldMappings);
  }

  public function mapperClassesData()
  {
    return [
      [
        'AnnotatedMapper',
        [
          'id'          => [
            'fieldName'  => 'id',
            'type'       => 'integer',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'id'         => true,
            'columnName' => 'id',
          ],
          'username'    => [
            'fieldName'  => 'username',
            'type'       => 'string',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'username',
          ],
          'displayName' => [
            'fieldName'  => 'displayName',
            'type'       => 'string',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'displayName',
          ],
          'createdOn'   => [
            'fieldName'  => 'createdOn',
            'type'       => 'date',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'createdOn',
          ],
          'lastLogin'   => [
            'fieldName'  => 'lastLogin',
            'type'       => 'datetime',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'lastLogin',
          ],
        ]
      ],
      [
        'PartAnnotatedMapper',
        [
          'displayName' => [
            'fieldName'  => 'displayName',
            'type'       => 'string',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'displayName',
          ],
          'lastLogin'   => [
            'fieldName'  => 'lastLogin',
            'type'       => 'datetime',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'lastLogin',
          ],
          'id'          => [
            'fieldName'     => 'id',
            'columnName'    => 'id',
            'type'          => 'integer',
            'id'            => true,
            'autoincrement' => true,
            'unsigned'      => true,
          ],
          'username'    => [
            'fieldName'  => 'username',
            'columnName' => 'username',
            'type'       => 'string',
          ],
          'createdOn'   => [
            'fieldName'  => 'createdOn',
            'columnName' => 'created_on',
            'type'       => 'date',
          ],
          'createdAt'   => [
            'fieldName'  => 'createdAt',
            'columnName' => 'created_at',
            'type'       => 'datetime',
          ],
          'updatedAt'   => [
            'fieldName'  => 'updatedAt',
            'columnName' => 'updated_at',
            'type'       => 'datetime',
          ],
        ]
      ],
      [
        'UnannotatedMapper',
        [
          'id'           => [
            'fieldName'     => 'id',
            'columnName'    => 'id',
            'type'          => 'integer',
            'id'            => true,
            'autoincrement' => true,
            'unsigned'      => true,
          ],
          'age'          => [
            'fieldName'  => 'age',
            'columnName' => 'age',
            'type'       => 'smallint',
          ],
          'notes'        => [
            'fieldName'  => 'notes',
            'columnName' => 'notes',
            'type'       => 'text',
          ],
          'price'        => [
            'fieldName'  => 'price',
            'columnName' => 'price',
            'type'       => 'decimal',
            'precision'  => 10,
            'scale'      => 2,
          ],
          'tax'          => [
            'fieldName'  => 'tax',
            'columnName' => 'tax',
            'type'       => 'decimal',
            'precision'  => 10,
            'scale'      => 2,
          ],
          'amount'       => [
            'fieldName'  => 'amount',
            'columnName' => 'amount',
            'type'       => 'decimal',
            'precision'  => 10,
            'scale'      => 2,
          ],
          'cost'         => [
            'fieldName'  => 'cost',
            'columnName' => 'cost',
            'type'       => 'decimal',
            'precision'  => 10,
            'scale'      => 2,
          ],
          'total'        => [
            'fieldName'  => 'total',
            'columnName' => 'total',
            'type'       => 'decimal',
            'precision'  => 10,
            'scale'      => 2,
          ],
          'createdAt'    => [
            'fieldName'  => 'createdAt',
            'columnName' => 'created_at',
            'type'       => 'datetime',
          ],
          'updateTime'   => [
            'fieldName'  => 'updateTime',
            'columnName' => 'update_time',
            'type'       => 'datetime',
          ],
          'lastViewedOn' => [
            'fieldName'  => 'lastViewedOn',
            'columnName' => 'last_viewed_on',
            'type'       => 'date',
          ],
          'expiryDate'   => [
            'fieldName'  => 'expiryDate',
            'columnName' => 'expiry_date',
            'type'       => 'date',
          ],
          'comment'      => [
            'fieldName'  => 'comment',
            'columnName' => 'comment',
            'type'       => 'string',
          ],
          'updatedAt'    => [
            'fieldName'  => 'updatedAt',
            'columnName' => 'updated_at',
            'type'       => 'datetime',
          ],
        ]
      ]
    ];
  }
}
