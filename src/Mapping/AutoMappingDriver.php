<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers\Mapping;

use Doctrine\Common\Inflector\Inflector;
use Doctrine\Common\Persistence\Mapping\ClassMetadata;
use Doctrine\Common\Persistence\Mapping\Driver\MappingDriver;
use Doctrine\ORM\Mapping\MappingException;
use Packaged\Mappers\BaseMapper;

class AutoMappingDriver implements MappingDriver
{
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
    if(! ($metadata instanceof \Doctrine\ORM\Mapping\ClassMetadata))
    {
      throw new \Exception('Error: class metadata object is the wrong type');
    }

    $refClass = new \ReflectionClass($className);
    if($refClass->getDocComment() == "")
    {
      $metadata->setPrimaryTable(['name' => $this->_getTableName($className)]);
    }

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
        $columnName = Inflector::tableize($propName);
        $metadata->mapField(
          [
            'fieldName' => $propName,
            'columnName' => $columnName,
            'type' => $this->_getDefaultDataType($columnName),
            'id' => $columnName == 'id'
          ]
        );
      }
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
    $nsParts      = explode('\\',$className);
    $ignoreFirst  = 1;

    foreach($nsParts as $i => $part)
    {
      if($i < $ignoreFirst || in_array(strtolower($part), $excludeParts))
      {
        unset($nsParts[$i]);
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
    $parts = explode("_", $columnName);
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

    if(! $type)
    {
      switch($lastPart)
      {
        case 'id':
          $type = 'integer';
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

    if(! $type)
    {
      $type = 'string';
    }

    return $type;
  }

  /**
   * Gets the names of all mapped classes known to this driver.
   *
   * @return array The names of all mapped classes known to this driver.
   */
  public function getAllClassNames()
  {
    // TODO: Implement getAllClassNames() method.
    return [];
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
    return !($className instanceof BaseMapper);
  }
}
