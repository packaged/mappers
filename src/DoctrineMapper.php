<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

use Doctrine\Common\Collections\Criteria;
use Doctrine\Common\Collections\Expr\Expression;
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
      /**
       * @var $obj static
       */
      $obj->setPersistedData(call_user_func('get_object_vars', $obj));
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
    $crit = Criteria::create();
    if($order)
    {
      $crit->orderBy($order);
    }
    $crit->setMaxResults($limit);
    $crit->setFirstResult($offset);
    foreach($criteria as $k => $v)
    {
      if($v instanceof Criteria)
      {
        $crit->andWhere($crit->getWhereExpression());
      }
      elseif($v instanceof Expression)
      {
        $crit->andWhere($v);
      }
      else
      {
        $expr = Criteria::expr();
        $crit->andWhere(
          $expr->andX($expr->eq($k, $v))
        );
      }
    }

    $entities = static::getEntityManager()->getRepository(get_called_class())
      ->matching($crit)->toArray();

    foreach($entities as $entity)
    {
      /**
       * @var $entity static
       */
      $entity->setExists(true);
      $entity->setPersistedData(call_user_func('get_object_vars', $entity));
    }
    return $entities;
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

    $changedFields = $this->getChangedFields();

    $changesMade = [];
    foreach($changedFields as $field => $value)
    {
      $changesMade[$field]          = [
        'from' => isset($this->_persistedData[$field])
          ? $this->_persistedData[$field] : null,
        'to'   => $value
      ];
      $this->_persistedData[$field] = $value;
    }
    $this->_savedChanges = $changesMade;
    return $changesMade;
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
    $new = new static();

    // hydrate new mapper with existing values
    $new->hydrate(call_user_func('get_object_vars', $this));

    // hydrate new mapper with specified keys
    $new->setId($newKey);

    // unset autoTimestamps
    if(static::useAutoTimestamp())
    {
      $new->updatedAt = null;
      $new->createdAt = null;
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
