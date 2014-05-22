<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

use Packaged\Mappers\Exceptions\InvalidLoadException;

/**
 * Class BaseMapper
 * @package Packaged\Mappers
 * @MappedSuperclass
 * @HasLifecycleCallbacks
 */
abstract class DoctrineMapper extends BaseMapper
{
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
      $obj = static::getEntityManager()->find(get_called_class(), $id);
      return $obj ? $obj->setExists(true) : new static();
    }
  }

  /**
   * @param array $criteria
   * @param null  $order
   * @param null  $limit
   * @param null  $offset
   *
   * @return array
   */
  public static function loadWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  )
  {
    return static::getEntityManager()->getRepository(get_called_class())
      ->findBy($criteria, $order, $limit, $offset);
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

    if(!$this->isCompositeId())
    {
      $keys = $this->_getKeys();
      $new->hydrate([reset($keys) => null]);
    }
    $new->save();
    return $new;
  }

  /**
   * @PostLoad
   */
  public function onLoadSetExists()
  {
    $this->setExists(true);
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

  public function increment($field, $count)
  {
    $em    = static::getEntityManager();
    $query = $em->createQuery(
      'UPDATE ' . get_class($this)
      . ' a SET a.' . $field . ' = a.' . $field . ' + :count'
    );
    $query->execute(['count' => $count]);
    $this->$field += $count;
    $em->persist($this);
  }

  public function decrement($field, $count)
  {
    $em    = static::getEntityManager();
    $query = $em->createQuery(
      'UPDATE ' . get_class($this)
      . ' a SET a.' . $field . ' = a.' . $field . ' - :count'
    );
    $query->execute(['count' => $count]);
    $this->$field -= $count;
    $em->persist($this);
  }
}
