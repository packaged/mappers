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

  public function save();

  public function saveAsNew($newKey = null);

  public function reload();

  public function delete();

  public function id();

  public function increment($field, $count);

  public function decrement($field, $count);

  public static function getTableName();
}
