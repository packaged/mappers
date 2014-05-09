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

  /**
   * @dataProvider mapperClassesData
   *
   * @param $className
   * @param $expectedFieldMappings
   *
   * @throws Exception
   */
  public function testMapperClass($className, $expectedFieldMappings)
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
          'createdAt'   => [
            'fieldName'  => 'createdAt',
            'type'       => 'datetime',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'createdAt',
          ],
          'updatedAt'   => [
            'fieldName'  => 'updatedAt',
            'type'       => 'datetime',
            'scale'      => 0,
            'length'     => null,
            'unique'     => false,
            'nullable'   => false,
            'precision'  => 0,
            'columnName' => 'updatedAt',
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
            'id'         => false,
          ],
          'createdOn'   => [
            'fieldName'  => 'createdOn',
            'columnName' => 'created_on',
            'type'       => 'date',
            'id'         => false,
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
            'id'         => false,
          ],
          'notes'        => [
            'fieldName'  => 'notes',
            'columnName' => 'notes',
            'type'       => 'text',
            'id'         => false,
          ],
          'price'        => [
            'fieldName'  => 'price',
            'columnName' => 'price',
            'type'       => 'decimal',
            'id'         => false,
            'precision'  => 10,
            'scale'      => 2,
          ],
          'tax'          => [
            'fieldName'  => 'tax',
            'columnName' => 'tax',
            'type'       => 'decimal',
            'id'         => false,
            'precision'  => 10,
            'scale'      => 2,
          ],
          'amount'       => [
            'fieldName'  => 'amount',
            'columnName' => 'amount',
            'type'       => 'decimal',
            'id'         => false,
            'precision'  => 10,
            'scale'      => 2,
          ],
          'cost'         => [
            'fieldName'  => 'cost',
            'columnName' => 'cost',
            'type'       => 'decimal',
            'id'         => false,
            'precision'  => 10,
            'scale'      => 2,
          ],
          'total'        => [
            'fieldName'  => 'total',
            'columnName' => 'total',
            'type'       => 'decimal',
            'id'         => false,
            'precision'  => 10,
            'scale'      => 2,
          ],
          'createdAt'    => [
            'fieldName'  => 'createdAt',
            'columnName' => 'created_at',
            'type'       => 'datetime',
            'id'         => false,
          ],
          'updateTime'   => [
            'fieldName'  => 'updateTime',
            'columnName' => 'update_time',
            'type'       => 'datetime',
            'id'         => false,
          ],
          'lastViewedOn' => [
            'fieldName'  => 'lastViewedOn',
            'columnName' => 'last_viewed_on',
            'type'       => 'date',
            'id'         => false,
          ],
          'expiryDate'   => [
            'fieldName'  => 'expiryDate',
            'columnName' => 'expiry_date',
            'type'       => 'date',
            'id'         => false,
          ],
          'comment'      => [
            'fieldName'  => 'comment',
            'columnName' => 'comment',
            'type'       => 'string',
            'id'         => false,
          ],
          'updatedAt'    => [
            'fieldName'  => 'updatedAt',
            'columnName' => 'updated_at',
            'type'       => 'datetime',
            'id'         => false,
          ],
        ]
      ]
    ];
  }
}
