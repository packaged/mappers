<?php
/**
 * Created by PhpStorm.
 * User: Tom Kay
 * Date: 20/05/2014
 * Time: 14:05
 */

namespace Packaged\Mappers;

interface IMapper
{
  /**
   * Load a mapper with a specific id
   *
   * @param mixed $id
   *
   * @return IMapper
   */
  public static function load($id);

  /**
   * @param array $criteria
   * @param null  $order
   * @param null  $limit
   * @param null  $offset
   *
   * @return IMapper[]
   */
  public static function loadWhere(
    array $criteria, $order = null, $limit = null, $offset = null
  );

  /**
   * @param array $criteria
   */
  public static function deleteWhere(array $criteria);

  /**
   * @return array changed columns previous values, keyed by propertyName
   */
  public function save();

  /**
   * @param $newKey
   *
   * @return $this
   */
  public function saveAsNew($newKey = null);

  /**
   * @return $this
   */
  public function reload();

  /**
   * @return $this
   */
  public function delete();

  public function id();

  /**
   * @return bool
   */
  public function exists();

  public function increment($field, $count);

  public function decrement($field, $count);

  public static function getTableName();

  public static function createTable();
}
