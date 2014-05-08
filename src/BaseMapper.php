<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

use Doctrine\ORM\Event\LoadClassMetadataEventArgs;
use Doctrine\ORM\Mapping\ClassMetadata;

/**
 * Class BaseMapper
 * @package Packaged\Mappers
 * @MappedSuperclass
 * @HasLifecycleCallbacks
 */
abstract class BaseMapper
{
  /**
   * @Column(type="datetime")
   */
  public $createdAt;

  /**
   * @Column(type="datetime")
   */
  public $updatedAt;

  /**
   * @returns bool
   */
  public static function getAutoTimestamp()
  {
    return true;
  }

  /**
   * @PrePersist
   */
  public function autoDateOnSave()
  {
    if(static::getAutoTimestamp())
    {
      $this->createdAt = new \DateTime('now');
      $this->updatedAt = new \DateTime('now');
    }
  }

  /**
   * @PreUpdate
   */
  public function autoDateOnUpdate()
  {
    if(static::getAutoTimestamp())
    {
      $this->updatedAt = new \DateTime('now');
    }
  }

  /**
   * @LoadClassMetadata
   */
  public function autoDateRemoveMetadata(LoadClassMetadataEventArgs $eventArgs)
  {
    //TODO: this method is not being called for some reason...
    $metadata = $eventArgs->getClassMetadata();
    if(!static::getAutoTimestamp() && $metadata instanceof ClassMetadata)
    {
      foreach($metadata->fieldMappings as $k => $mapping)
      {
        if($mapping['fieldName'] === 'createdAt' ||
          $mapping['fieldName'] === 'updatedAt'
        )
        {
          unset($metadata->fieldMappings[$k]);
        }
      }
    }
  }

  public function __construct()
  {
    if(func_get_args())
    {
      throw new \Exception('Cannot construct with ID.  Use Class::load($id)');
    }
  }

  protected static $_resolver;

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

  protected static $_service = 'db';

  /**
   * @return \Doctrine\ORM\EntityManager
   */
  public static function getEntityManager()
  {
    return static::getConnectionResolver()->getConnection(static::$_service);
  }

  /**
   * @param mixed $id
   *
   * @return static
   * @throws \Doctrine\ORM\TransactionRequiredException
   * @throws \Doctrine\ORM\ORMException
   * @throws \Doctrine\ORM\OptimisticLockException
   * @throws \Doctrine\ORM\ORMInvalidArgumentException
   */
  public static function load($id = null)
  {
    if($id === null)
    {
      return new static();
    }
    else
    {
      $obj = static::getEntityManager()->find(get_called_class(), $id);
      return $obj ? $obj->setExists(true) : new static();
    }
  }

  public static function loadFromMaster($id = null)
  {
    //TODO: use Master EntityManager
    static::load($id);
  }

  public function save()
  {
    $em = static::getEntityManager();
    $em->persist($this);
    $em->flush();
    $this->setExists(true);
  }

  /**
   * @return static
   * @throws \Doctrine\ORM\Mapping\MappingException
   */
  public function saveAsNew()
  {
    // timestamps
    $new = new static();
    foreach($this->_getMetadata()->fieldNames as $field)
    {
      $new->$field = $this->$field;
    }
    $keys = $this->_getKeys();
    if(count($keys) === 1)
    {
      $new->hydrate([reset($keys) => null]);
    }
    $new->save();
    return $new;
  }

  public function hydrate(array $values)
  {
    foreach($values as $key => $value)
    {
      $this->$key = $value;
    }
  }

  protected $_exists = false;

  public function setExists($bool = true)
  {
    $this->_exists = $bool;
    return $this;
  }

  public function exists()
  {
    return $this->_exists;
  }

  public function reload()
  {
    static::getEntityManager()->refresh($this);
    return $this;
  }

  public function delete()
  {
    static::getEntityManager()->remove($this);
    static::getEntityManager()->flush($this);
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

  protected function _getMetadata()
  {
    return static::getEntityManager()->getClassMetadata(get_class($this));
  }

  protected function _getKeys()
  {
    return $this->_getMetadata()->getIdentifierColumnNames();
  }

  protected function _getKeyValues()
  {
    return $this->_getMetadata()->getIdentifierValues($this);
  }

  /**
   * @return bool
   */
  public function isCompositeId()
  {
    return count($this->id()) > 1;
  }
} 