<?php
use Doctrine\ORM\Mapping\ClassMetadata;
use Packaged\Mappers\Mapping\AutoMappingDriver;

require_once __DIR__ . '/MockClassMetadata.php';

/**
 * Class AutoMappingDriverTest
 */
class AutoMappingDriverTest extends PHPUnit_Framework_TestCase
{
  private $_loadedMapperClasses = false;

  public function testAllClassNames()
  {
    $driver = new AutoMappingDriver(__DIR__ . '/MapperClasses');

    $expected = [
      'MappersTests\Applications\MyApp\ApplicationMapper',
      'MappersTests\Components\TestComponent\ComponentsMapper',
      'MappersTests\Mappers\MappersMapper',
      'MappersTests\Modules\TestModule\ModulesMapper',
      'NotNamespacedMapper'
    ];
    $found    = $driver->getAllClassNames();

    // The order doesn't matter so sort both
    sort($expected);
    sort($found);
    $this->assertEquals($expected, $found);
  }

  /**
   * @dataProvider tableNamesData
   *
   * @param string $className
   * @param string $tableName
   */
  public function testTableName($className, $tableName)
  {
    $this->_loadMapperClasses();

    $meta   = new ClassMetadata($className);
    $driver = new AutoMappingDriver();
    $driver->loadMetadataForClass($className, $meta);

    $this->assertArrayHasKey('name', $meta->table);
    $this->assertEquals(
      $tableName,
      $meta->getTableName(),
      'Table name incorrect: expected="' . $tableName
      . '", found="' . $meta->getTableName() . '"'
    );
  }

  public function tableNamesData()
  {
    return [
      [
        'MappersTests\Applications\MyApp\ApplicationMapper',
        'my_app_application_mappers'
      ],
      [
        'MappersTests\Components\TestComponent\ComponentsMapper',
        'test_component_components_mappers'
      ],
      ['MappersTests\Mappers\MappersMapper', 'mappers_mappers'],
      [
        'MappersTests\Modules\TestModule\ModulesMapper',
        'test_module_modules_mappers'
      ],
      ['NotNamespacedMapper', 'not_namespaced_mappers']
    ];
  }

  private function _loadMapperClasses()
  {
    if(!$this->_loadedMapperClasses)
    {
      $mappersDir = __DIR__ . '/MapperClasses';
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
      $this->_loadedMapperClasses = true;
    }
  }

  public function testFieldMappings()
  {
    $this->_loadMapperClasses();

    $className = 'NotNamespacedMapper';
    $meta      = new ClassMetadata($className);
    $driver    = new AutoMappingDriver();
    $driver->loadMetadataForClass($className, $meta);

    $this->assertEquals(
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
        ]
      ],
      $meta->fieldMappings
    );

    $this->assertEquals(
      [
        'id'             => 'id',
        'age'            => 'age',
        'notes'          => 'notes',
        'price'          => 'price',
        'tax'            => 'tax',
        'amount'         => 'amount',
        'cost'           => 'cost',
        'total'          => 'total',
        'created_at'     => 'createdAt',
        'update_time'    => 'updateTime',
        'last_viewed_on' => 'lastViewedOn',
        'expiry_date'    => 'expiryDate',
        'comment'        => 'comment',
        'updated_at'     => 'updatedAt'
      ],
      $meta->fieldNames
    );

    $this->assertEquals(
      [
        'id'           => 'id',
        'age'          => 'age',
        'notes'        => 'notes',
        'price'        => 'price',
        'tax'          => 'tax',
        'amount'       => 'amount',
        'cost'         => 'cost',
        'total'        => 'total',
        'createdAt'    => 'created_at',
        'updateTime'   => 'update_time',
        'lastViewedOn' => 'last_viewed_on',
        'expiryDate'   => 'expiry_date',
        'comment'      => 'comment',
        'updatedAt'    => 'updated_at'
      ],
      $meta->columnNames
    );
  }

  /**
   * @expectedException Exception
   * @expectedExceptionMessage Error: class metadata object is the wrong type
   */
  public function testInvalidClassMetadataObject()
  {
    (new AutoMappingDriver())
      ->loadMetadataForClass('TestClass', new MockClassMetadata());
  }
}
