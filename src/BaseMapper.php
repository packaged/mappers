<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

class BaseMapper
{
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
    $metadata = $this->_getMetadata();
    $new      = new static();
    foreach($metadata->fieldNames as $field)
    {
      $new->$field = $this->$field;
    }
    foreach($new->_getKeys() as $key)
    {
      $map = $metadata->getFieldMapping($key);
      if(isset($map['id']) && $map['id'])
      {
        $new->$key = null;
      }
    }
    $new->save();
    return $new;
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
    if(count($vals) === 1)
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