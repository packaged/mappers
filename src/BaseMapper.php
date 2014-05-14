<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 07/05/2014
 * Time: 10:33
 */

namespace Packaged\Mappers;

use Packaged\Mappers\Exceptions\InvalidLoadException;
use Respect\Validation\Validator;

/**
 * Class BaseMapper
 * @package Packaged\Mappers
 * @MappedSuperclass
 * @HasLifecycleCallbacks
 */
abstract class BaseMapper
{
  /**
   * @var \DateTime|null
   */
  public $createdAt;
  /**
   * @var \DateTime|null
   */
  public $updatedAt;

  protected static $_resolver;
  protected $_exists = false;
  /**
   * @var Validator[]
   */
  protected $_validators = [];

  protected $_validationErrors = [];

  public static function getService()
  {
    return 'db';
  }

  /**
   * @returns bool
   */
  public static function getAutoTimestamp()
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

  /**
   * @PrePersist
   */
  public function prePersist()
  {
    if(static::getAutoTimestamp())
    {
      $this->createdAt = new \DateTime('now');
      $this->updatedAt = new \DateTime('now');
    }
    $this->validate();
  }

  /**
   * @PreUpdate
   */
  public function preUpdate()
  {
    if(static::getAutoTimestamp())
    {
      $this->updatedAt = new \DateTime('now');
    }
    $this->validate();
  }

  /**
   * Validate the whole entity or a single field
   *
   * @param bool       $throwExceptions If true then throw exceptions if
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

  /**
   * @return \Doctrine\ORM\EntityManager
   */
  public static function getEntityManager()
  {
    return static::getConnectionResolver()->getConnection(static::getService());
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

  public function hydrate(array $values)
  {
    foreach($values as $key => $value)
    {
      $this->$key = $value;
    }
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

  /**
   * @param string    $fieldName
   * @param Validator $validator
   */
  protected function _addValidator($fieldName, Validator $validator)
  {
    $this->_validators[$fieldName] = $validator;
  }
}
