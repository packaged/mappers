<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 20/05/2014
 * Time: 15:34
 */

namespace Packaged\Mappers;

use Doctrine\ORM\Configuration;
use Doctrine\ORM\Mapping\ClassMetadata;
use Packaged\Mappers\Exceptions\InvalidLoadException;
use Packaged\Mappers\Mapping\AutoMappingDriver;
use Packaged\Mappers\Mapping\ChainedDriver;
use Respect\Validation\Validator;

abstract class BaseMapper implements IMapper
{
  /**
   * @var \DateTime|null
   */
  public $createdAt;
  /**
   * @var \DateTime|null
   */
  public $updatedAt;

  protected static $_autoTimeFormat = self::AUTOTIME_FORMAT_DATETIME;
  const AUTOTIME_FORMAT_DATETIME = 'datetime';
  const AUTOTIME_FORMAT_TIMESTAMP = 'timestamp';
  const AUTOTIME_FORMAT_MILLISECONDS = 'milliseconds';

  protected static $_resolver;
  protected $_exists = false;
  /**
   * @var Validator[]
   */
  protected $_validators = [];

  protected $_validationErrors = [];

  public static function getServiceName()
  {
    return 'db';
  }

  /**
   * @returns bool
   */
  public static function useAutoTimestamp()
  {
    return true;
  }

  public static function getCreatedAtColumn()
  {
    return 'created_at';
  }

  public static function getUpdatedAtColumn()
  {
    return 'updated_at';
  }

  protected static function _getAutoTime()
  {
    if(static::$_autoTimeFormat == self::AUTOTIME_FORMAT_DATETIME)
    {
      return new \DateTime('now');
    }
    if(static::$_autoTimeFormat == self::AUTOTIME_FORMAT_TIMESTAMP)
    {
      return time();
    }
    if(static::$_autoTimeFormat == self::AUTOTIME_FORMAT_MILLISECONDS)
    {
      return floor(microtime(true) * 1000);
    }
    return time();
  }

  public function touch()
  {
    $time = static::_getAutoTime();
    if(!$this->createdAt)
    {
      $this->createdAt = $time;
    }
    $this->updatedAt = $time;
  }

  public function preCreate()
  {
    if(static::useAutoTimestamp())
    {
      $this->touch();
    }
    $this->validate();
  }

  public function preUpdate()
  {
    if(static::useAutoTimestamp())
    {
      $this->touch();
    }
    $this->validate();
  }

  /**
   * Validate the whole entity or a single field
   *
   * @param bool $throwExceptions       If true then throw exceptions if
   *                                    validation fails, otherwise return
   *                                    true/false
   *
   * @return bool
   */
  public function validate($throwExceptions = true)
  {
    $allOk = true;
    foreach($this->_validators as $fieldName => $validator)
    {
      if(property_exists($this, $fieldName))
      {
        if($throwExceptions)
        {
          $validator->assert($this->$fieldName);
        }
        else
        {
          if(!$validator->validate($this->$fieldName))
          {
            $allOk = false;
          }
        }
      }
    }
    return $allOk;
  }

  final public static function getTableName()
  {
    return static::_getMetadata()->getTableName();
  }

  private static $classMetadata = [];

  /**
   * @return ClassMetadata
   * @throws \Exception
   */
  protected static function _getMetadata()
  {
    $className = get_called_class();
    if(!isset(self::$classMetadata[$className]))
    {
      self::$classMetadata[$className] = new ClassMetadata($className);
      $driver                          = new ChainedDriver(
        [
          (new Configuration())->newDefaultAnnotationDriver(),
          new AutoMappingDriver('string')
        ]
      );
      $driver->loadMetadataForClass(
        $className,
        self::$classMetadata[$className]
      );
    }
    return self::$classMetadata[$className];
  }

  /**
   * returns an array of columnName => fieldName
   * @return array
   */
  protected static function _getColumnMap()
  {
    return static::_getMetadata()->fieldNames;
  }

  /**
   * returns an array of fieldName => columnName
   * @return array
   */
  protected static function _getFieldMap()
  {
    return static::_getMetadata()->columnNames;
  }

  protected static function getColumnName($field)
  {
    return self::_getFieldMap()[$field];
  }

  protected static function getFieldName($column)
  {
    return self::_getColumnMap()[$column];
  }

  /**
   * @param $id
   *
   * @return static
   */
  public static function loadOrNew($id)
  {
    try
    {
      $mapper = static::load($id);
    }
    catch(\Exception $e)
    {
      $mapper = new static();
      $mapper->setId($id);
    }
    return $mapper;
  }

  /**
   * @param array $criteria
   * @param null  $order
   * @param null  $limit
   * @param null  $offset
   *
   * @return static
   * @throws \Exception
   */
  public static function loadOneWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  )
  {
    $rows = static::loadWhere($criteria, $order, 2, $offset);
    if(!$rows)
    {
      return null;
    }
    elseif(count($rows) > 1)
    {
      throw new \Exception('More than one record found.');
    }

    return $rows[0];
  }

  public function id()
  {
    $vals = $this->_getKeyValues();
    if(is_array($vals) && count($vals) === 1)
    {
      return reset($vals);
    }
    return $vals;
  }

  public function setId($value)
  {
    foreach(array_combine(static::_getKeyColumns(), (array)$value) as $k => $v)
    {
      $this->$k = $v;
    }
  }

  protected static function _getKeyColumns()
  {
    return static::_getMetadata()->getIdentifierColumnNames();
  }

  protected static function _getKeyFields()
  {
    return static::_getMetadata()->getIdentifierFieldNames();
  }

  protected function _getKeyValues()
  {
    $values = [];
    foreach(static::_getKeyColumns() as $key)
    {
      $values[$key] = $this->$key;
    }
    return $values;
  }

  public function validateField($fieldName, $throwExceptions = true)
  {
    $result = true;
    if(isset($this->_validators[$fieldName])
      && property_exists($this, $fieldName)
    )
    {
      $validator = $this->_validators[$fieldName];
      if($throwExceptions)
      {
        $validator->assert($this->$fieldName);
      }
      else
      {
        $result = $validator->validate($this->$fieldName);
      }
    }
    return $result;
  }

  /**
   * @param string    $fieldName
   * @param Validator $validator
   */
  protected function _addValidator($fieldName, Validator $validator)
  {
    $this->_validators[$fieldName] = $validator;
  }

  public function __construct()
  {
    if(func_get_args())
    {
      throw new InvalidLoadException(
        'Cannot construct with ID.  Use Class::load($id)'
      );
    }

    $this->_configure();
  }

  protected function _configure()
  {
  }

  public static function setConnectionResolver(IConnectionResolver $resolver)
  {
    static::$_resolver = $resolver;
  }

  /**
   * @return IConnectionResolver
   */
  public static function getConnectionResolver()
  {
    return static::$_resolver;
  }

  public function hydrate(array $values)
  {
    $map = static::_getColumnMap();
    foreach($values as $key => $value)
    {
      $this->$map[$key] = $value;
    }
    return $this;
  }

  public function setExists($bool = true)
  {
    $this->_exists = $bool;
    return $this;
  }

  public function exists()
  {
    return $this->_exists;
  }
}