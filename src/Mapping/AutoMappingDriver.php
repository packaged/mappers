<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers\Mapping;

use Doctrine\Common\Inflector\Inflector;
use Doctrine\Common\Persistence\Mapping\ClassMetadata;
use Doctrine\Common\Persistence\Mapping\Driver\StaticPHPDriver;
use Doctrine\ORM\Mapping\MappingException;

class AutoMappingDriver extends StaticPHPDriver
{
  protected $_idType;

  public function __construct($idType = 'integer', $paths = null)
  {
    $this->_idType = $idType;
    if(!$paths)
    {
      $paths = [];
    }
    parent::__construct((array)$paths);
  }

  /**
   * Loads the metadata for the specified class into the provided container.
   *
   * @param string        $className
   * @param ClassMetadata $metadata
   *
   * @throws \Exception
   * @return void
   */
  public function loadMetadataForClass($className, ClassMetadata $metadata)
  {
    if(!($metadata instanceof \Doctrine\ORM\Mapping\ClassMetadata))
    {
      throw new \Exception('Error: class metadata object is the wrong type');
    }
    $refClass      = new \ReflectionClass($className);
    $classDocBlock = $refClass->getDocComment();
    if((!$classDocBlock) || (strpos($classDocBlock, '@Table') === false))
    {
      $metadata->setPrimaryTable(['name' => $this->_getTableName($className)]);
    }

    $needAutoGenerator = false;

    foreach($refClass->getProperties(\ReflectionProperty::IS_PUBLIC) as $prop)
    {
      $propName = $prop->getName();

      try
      {
        $mapping = $metadata->getFieldMapping($propName);
      }
      catch(MappingException $e)
      {
        $mapping = null;
      }

      if(!$mapping)
      {
        if($propName == 'createdAt')
        {
          if(!$this->isTransient($className) && !$refClass->isAbstract() &&
            call_user_func($className . '::useAutoTimestamp')
          )
          {
            $metadata->mapField(
              [
                'fieldName'  => 'createdAt',
                'columnName' => call_user_func(
                  $className . '::getCreatedAtColumn'
                ),
                'type'       => 'datetime',
              ]
            );
          }
        }
        else if($propName == 'updatedAt')
        {
          if(!$this->isTransient($className) && !$refClass->isAbstract() &&
            call_user_func($className . '::useAutoTimestamp')
          )
          {
            $metadata->mapField(
              [
                'fieldName'  => 'updatedAt',
                'columnName' => call_user_func(
                  $className . '::getUpdatedAtColumn'
                ),
                'type'       => 'datetime',
              ]
            );
          }
        }
        else
        {
          $columnName = Inflector::tableize($propName);

          $fieldMap = [
            'fieldName'  => $propName,
            'columnName' => $columnName,
            'type'       => $this->_getDefaultDataType($columnName)
          ];
          if($columnName == 'id')
          {
            $fieldMap['id']            = true;
            $fieldMap['autoincrement'] = true;
            $fieldMap['unsigned']      = true;
            $needAutoGenerator         = true;
          }
          else if(in_array(
            $columnName,
            ['price', 'tax', 'amount', 'cost', 'total']
          )
          )
          {
            // DECIMAL(10,2)
            $fieldMap['precision'] = 10;
            $fieldMap['scale']     = 2;
          }
          $metadata->mapField($fieldMap);
        }
      }
    }

    if($needAutoGenerator && (!$metadata->usesIdGenerator()))
    {
      $metadata->setIdGeneratorType(
        \Doctrine\ORM\Mapping\ClassMetadata::GENERATOR_TYPE_AUTO
      );
    }
  }

  /**
   * Work out the default table name from the namespace and class name
   *
   * @param string $className
   *
   * @return string
   */
  private function _getTableName($className)
  {
    $excludeParts = [
      'mappers',
      'applications',
      'modules',
      'components'
    ];
    $nsParts      = explode('\\', $className);

    if(count($nsParts) > 1)
    {
      $ignoreFirst = 1;
      foreach($nsParts as $i => $part)
      {
        if($i < $ignoreFirst || in_array(strtolower($part), $excludeParts))
        {
          unset($nsParts[$i]);
        }
      }
    }
    return Inflector::tableize(Inflector::pluralize(implode('', $nsParts)));
  }

  /**
   * Work out the default datatype for a column based on its name
   *
   * @param string $columnName
   *
   * @return string
   */
  private function _getDefaultDataType($columnName)
  {
    $parts    = explode("_", $columnName);
    $lastPart = end($parts);

    $type = null;
    switch($columnName)
    {
      case 'age':
        $type = 'smallint';
        break;
      case 'notes':
        $type = 'text';
        break;
      case 'price':
      case 'tax':
      case 'amount':
      case 'cost':
      case 'total':
        $type = 'decimal';
        break;
    }

    if(!$type)
    {
      switch($lastPart)
      {
        case 'id':
          $type = $this->_idType;
          break;
        case 'at':
        case 'time':
          $type = 'datetime';
          break;
        case 'on':
        case 'date':
          $type = 'date';
          break;
      }
    }

    if(!$type)
    {
      $type = 'string';
    }

    return $type;
  }

  /**
   * Returns whether the class with the specified name should have its metadata loaded.
   * This is only the case if it is either mapped as an Entity or a MappedSuperclass.
   *
   * @param string $className
   *
   * @return boolean
   */
  public function isTransient($className)
  {
    return !is_subclass_of($className, 'Packaged\Mappers\BaseMapper');
  }
}
