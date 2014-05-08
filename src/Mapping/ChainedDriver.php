<?php
/**
 * @author  Richard.Gooding
 */

namespace Packaged\Mappers\Mapping;

use Doctrine\Common\Persistence\Mapping\ClassMetadata;
use Doctrine\Common\Persistence\Mapping\Driver\MappingDriver;

class ChainedDriver implements MappingDriver
{
  protected $_drivers = [];
  protected $_paths = [];

  /**
   * @param MappingDriver[] $drivers
   * @param string[] $paths
   */
  public function __construct(array $drivers, array $paths = null)
  {
    $this->_drivers = $drivers;
    if($paths)
    {
      $this->addPaths($paths);
    }
  }

  /**
   * @param array $paths
   */
  public function addPaths(array $paths)
  {
    $oldPaths     = $this->_paths;
    $this->_paths = array_unique(array_merge($this->_paths, $paths));
    $addedPaths   = array_diff($this->_paths, $oldPaths);

    if(count($addedPaths) > 0)
    {
      foreach($this->_drivers as $driver)
      {
        if(method_exists($driver, 'addPaths'))
        {
          $driver->addPaths($addedPaths);
        }
      }
    }
  }

  public function getPaths()
  {
    return $this->_paths;
  }

  public function loadMetadataForClass($className, ClassMetadata $metadata)
  {
    foreach($this->_drivers as $driver)
    {
      $driver->loadMetadataForClass($className, $metadata);
    }
  }

  public function isTransient($className)
  {
    $transient = true;
    foreach($this->_drivers as $driver)
    {
      if(!$driver->isTransient($className))
      {
        $transient = false;
        break;
      }
    }
    return $transient;
  }

  public function getAllClassNames()
  {
    $classes = [];
    foreach($this->_drivers as $driver)
    {
      $classes = array_unique(
        array_merge($classes, $driver->getAllClassNames())
      );
    }
    return $classes;
  }
}
