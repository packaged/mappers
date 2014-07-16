<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

use Doctrine\ORM\Tools\SchemaTool;
use Packaged\Mappers\Exceptions\InvalidLoadException;

/**
 * Class DoctrineMapper
 * @package Packaged\Mappers
 * @MappedSuperclass
 * @HasLifecycleCallbacks
 */
abstract class DoctrineMapper extends BaseMapper
{
  /**
   * @return \Doctrine\ORM\EntityManager
   */
  public static function getEntityManager()
  {
    return static::getConnectionResolver()->getConnection(
      static::getServiceName()
    );
  }

  /**
   * @param mixed $id
   *
   * @return static
   * @throws \Doctrine\ORM\TransactionRequiredException
   * @throws \Doctrine\ORM\ORMException
   * @throws \Doctrine\ORM\OptimisticLockException
   * @throws \Doctrine\ORM\ORMInvalidArgumentException
   * @throws InvalidLoadException
   */
  public static function load($id)
  {
    if($id === null)
    {
      throw new InvalidLoadException('No ID passed to load');
    }
    else
    {
      if(is_array($id) && isset($id[0]))
      {
        $idArray = array_combine(static::_getKeyFields(), (array)$id);
      }
      else
      {
        $idArray = $id;
      }
      $obj = static::getEntityManager()->find(get_called_class(), $idArray);
      if(!$obj)
      {
        throw new InvalidLoadException('No object found with that ID');
      }
      return $obj->setExists(true);
    }
  }

  /**
   * @param array $criteria
   * @param null  $order
   * @param null  $limit
   * @param null  $offset
   *
   * @return static[]
   */
  public static function loadWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  )
  {
    return static::getEntityManager()->getRepository(get_called_class())
      ->findBy($criteria, $order, $limit, $offset);
  }

  /**
   * @param array $criteria
   *
   * @throws \Exception
   */
  public static function deleteWhere(array $criteria)
  {
    throw new \Exception('Not yet implemented');
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
    return $this;
  }

  /**
   * @PrePersist
   */
  public function preCreate()
  {
    parent::preCreate();
  }

  /**
   * @PreUpdate
   */
  public function preUpdate()
  {
    parent::preUpdate();
  }

  /**
   * @PostLoad
   */
  public function postLoad()
  {
    $this->setExists(true);
  }

  /**
   * @param $newKey
   *
   * @return static
   */
  public function saveAsNew($newKey = null)
  {
    // timestamps
    $new = new static();
    foreach($this->_getColumnMap() as $field)
    {
      $new->$field = $this->$field;
    }

    if(!$this->isCompositeId())
    {
      $keys = $this->_getKeyColumns();
      $new->hydrate([reset($keys) => $newKey]);
    }
    $new->save();
    return $new;
  }

  public function reload()
  {
    static::getEntityManager()->refresh($this);
    return $this;
  }

  public function delete()
  {
    if($this->exists())
    {
      static::getEntityManager()->remove($this);
      static::getEntityManager()->flush($this);
      $this->setExists(false);
    }
    return $this;
  }

  protected static function _getMetadata()
  {
    return static::getEntityManager()->getClassMetadata(get_called_class());
  }

  /**
   * @return bool
   */
  public function isCompositeId()
  {
    return count($this->id()) > 1;
  }

  public function increment($field, $count)
  {
    $em       = static::getEntityManager();
    $keys     = $this->_getKeyValues();
    $keyArray = [];
    foreach($keys as $k => $v)
    {
      $keyArray[] = 'a.' . $k . ' = :' . $k;
    }
    $query         = $em->createQuery(
      'UPDATE ' . get_class($this)
      . ' a SET a.' . $field . ' = a.' . $field . ' + :count WHERE '
      . implode(' AND ', $keyArray)
    );
    $keys['count'] = $count;
    $query->execute($keys);
    $this->$field += $count;
    $em->persist($this);
  }

  public function decrement($field, $count)
  {
    $em       = static::getEntityManager();
    $keys     = $this->_getKeyValues();
    $keyArray = [];
    foreach($keys as $k => $v)
    {
      $keyArray[] = 'a.' . $k . ' = :' . $k;
    }
    $query         = $em->createQuery(
      'UPDATE ' . get_class($this)
      . ' a SET a.' . $field . ' = a.' . $field . ' - :count WHERE '
      . implode(' AND ', $keyArray)
    );
    $keys['count'] = $count;
    $query->execute($keys);
    $this->$field -= $count;
    $em->persist($this);
  }

  public static function createTable()
  {
    $em      = static::getEntityManager();
    $tool    = new SchemaTool($em);
    $classes = [$em->getClassMetadata(get_called_class())];
    $tool->updateSchema($classes, true);
  }
}
