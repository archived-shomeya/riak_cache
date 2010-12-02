<?php
// $Id$

function _riak_cache_client() {
  static $riak;
  if (!isset($riak)) {
    $connection = variable_get('riak_connection', array('host' => 'localhost', 'port' => '8098'));
    include_once dirname(__FILE__) .'/riak-php-client/riak.php';
    include_once dirname(__FILE__) .'/RiakCacheMapReduce.inc';
    $riak = new RiakClient($connection['host'], $connection['port']);
  }
  return $riak;
}

class DrupalRiakCache implements DrupalCacheInterface {

  function __construct($bin) {
    $this->bin = $bin;
  }

  function get($cid) {
    try {
      // Garbage collection necessary when enforcing a minimum cache lifetime.
      $this->garbageCollection($this->bin);
      $riak = _riak_cache_client();
      $bucket = $riak->bucket($this->bin);
      $object = $bucket->get($cid);
      $cache = $object->getData();
      return $this->prepareItem($cache);
    }
    catch (Exception $e) {
      watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
      // If the database is never going to be available, cache requests should
      // return FALSE in order to allow exception handling to occur.
      return FALSE;
    }
    return FALSE;
  }

  function getMultiple(&$cids) {
    try {
      $cache = array();
      // Garbage collection necessary when enforcing a minimum cache lifetime.
      $this->garbageCollection($this->bin);
      $riak = _riak_cache_client();
      $bucket = $riak->bucket($this->bin);
      
      foreach ($cids as $cid) {
        $object = $bucket->get($cid);
        $item = $object->getData();
        $item = $this->prepareItem($item);
        if ($item) {
          $cache[$item->cid] = $item;
        }
      }
      $cids = array_diff($cids, array_keys($cache));
      return $cache;
    }
    catch (Exception $e) {
      watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
      // If the database is never going to be available, cache requests should
      // return FALSE in order to allow exception handling to occur.
      return array();
    }
  }

  /**
   * Garbage collection for get() and getMultiple().
   *
   * @param $bin
   *   The bin being requested.
   */
  protected function garbageCollection() {
    global $user;

    // Garbage collection necessary when enforcing a minimum cache lifetime.
    $cache_flush = variable_get('cache_flush_' . $this->bin, 0);
    if ($cache_flush && ($cache_flush + variable_get('cache_lifetime', 0) <= REQUEST_TIME)) {
      // Reset the variable immediately to prevent a meltdown in heavy load situations.
      variable_set('cache_flush_' . $this->bin, 0);
      // Time to flush old cache data
      try {
        $riak = _riak_cache_client();
        $bucket = $riak->bucket($this->bin);

        $mapreduce = new RiakCacheMapReduce($riak);
        $cids = $mapreduce->add($this->bin)->map('function(v) {
          var r = JSON.parse(v.values[0].data);
          if (r.expire != '. CACHE_PERMANENT .' && r.expire < '. REQUEST_TIME .') {
            return [v.key];
          }
          else {
            return [];
          }
        }')->run();

        foreach ($cids as $key) {
          $object = $bucket->get($key);
          $object->delete();
        }
      }
      catch (Exception $e) {
        watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
      }

    }
  }

  /**
   * Prepare a cached item.
   *
   * Checks that items are either permanent or did not expire, and unserializes
   * data as appropriate.
   *
   * @param $cache
   *   An item loaded from cache_get() or cache_get_multiple().
   * @return
   *   The item with data unserialized as appropriate or FALSE if there is no
   *   valid item to load.
   */
  protected function prepareItem($cache) {
    global $user;

    if (is_array($cache)) {
      $cache = (object) $cache;
    }

    if (!isset($cache->data)) {
      return FALSE;
    }
    // If enforcing a minimum cache lifetime, validate that the data is
    // currently valid for this user before we return it by making sure the cache
    // entry was created before the timestamp in the current session's cache
    // timer. The cache variable is loaded into the $user object by _drupal_session_read()
    // in session.inc. If the data is permanent or we're not enforcing a minimum
    // cache lifetime always return the cached data.
    if ($cache->expire != CACHE_PERMANENT && variable_get('cache_lifetime', 0) && $user->cache > $cache->created) {
      // This cache data is too old and thus not valid for us, ignore it.
      return FALSE;
    }

    if ($cache->serialized) {
      $cache->data = unserialize($cache->data);
    }

    return $cache;
  }

  function set($cid, $data, $expire = CACHE_PERMANENT) {
    $fields = array(
      'created' => REQUEST_TIME,
      'expire' => $expire,
      'cid' => $cid,
    );
    if (!is_string($data)) {
      $fields['data'] = serialize($data);
      $fields['serialized'] = 1;
    }
    else {
      $fields['data'] = $data;
      $fields['serialized'] = 0;
    }

    try {
      $riak = _riak_cache_client();
      $bucket = $riak->bucket($this->bin);
      $object = $bucket->newObject($cid, $fields);
      $object->store();
    }
    catch (Exception $e) {
      watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
      // The database may not be available, so we'll ignore cache_set requests.
    }
  }

  function clear($cid = NULL, $wildcard = FALSE) {
    global $user;
    $riak = _riak_cache_client();
    $bucket = $riak->bucket($this->bin);

    if (empty($cid)) {
      if (variable_get('cache_lifetime', 0)) {
        // We store the time in the current user's $user->cache variable which
        // will be saved into the sessions bin by _drupal_session_write(). We then
        // simulate that the cache was flushed for this user by not returning
        // cached data that was cached before the timestamp.
        $user->cache = REQUEST_TIME;

        $cache_flush = variable_get('cache_flush_' . $this->bin, 0);
        if ($cache_flush == 0) {
          // This is the first request to clear the cache, start a timer.
          variable_set('cache_flush_' . $this->bin, REQUEST_TIME);
        }
        elseif (REQUEST_TIME > ($cache_flush + variable_get('cache_lifetime', 0))) {
          // Clear the cache for everyone, cache_lifetime seconds have
          // passed since the first request to clear the cache.
          try {
            $mapreduce = new RiakCacheMapReduce($riak);
            $cids = $mapreduce->add($this->bin)->map('function(v) {
              var r = JSON.parse(v.values[0].data);
              if (r.expire != '. CACHE_PERMANENT .' && r.expire < '. REQUEST_TIME .') {
                return [v.key];
              }
              else {
                return [];
              }
            }')->run();

            foreach ($cids as $key) {
              $object = $bucket->get($key);
              $object->delete();
            }
          }
          catch (Exception $e) {
            watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
          }

          variable_set('cache_flush_' . $this->bin, 0);
        }
      }
      else {
        // No minimum cache lifetime, flush all temporary cache entries now.
        try {
          $mapreduce = new RiakCacheMapReduce($riak);
          $cids = $mapreduce->add($this->bin)->map('function(v) {
            var r = JSON.parse(v.values[0].data);
            if (r.expire != '. CACHE_PERMANENT .' && r.expire < '. REQUEST_TIME .') {
              return [v.key];
            }
            else {
              return [];
            }
          }')->run();

          foreach ($cids as $key) {
            $object = $bucket->get($key);
            $object->delete();
          }
        }
        catch (Exception $e) {
          watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
        }
      }
    }
    else {
      if ($wildcard) {
        if ($cid == '*') {
          try {
            $keys = $bucket->getKeys();
            foreach ($keys as $key) {
              $object = $bucket->get($key);
              $object->delete();
            }
          }
          catch (Exception $e) {
            watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
          }
        }
        else {
          // @TODO, make like queries work
        }
      }
      elseif (is_array($cid)) {
        try {
          foreach ($cid as $key) {
            $object = $bucket->get($key);
            $object->delete();
          }
        }
        catch (Exception $e) {
          watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
        }

      }
      else {
        try {
          $object = $bucket->get($cid);
          $object->delete();
        }
        catch (Exception $e) {
          watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
        }
      }
    }
  }

  function isEmpty() {
    $this->garbageCollection();
    $keys = array();

    try {
      $riak = _riak_cache_client();
      $bucket = $riak->bucket($this->bin);
      $keys = $bucket->getKeys();
    }
    catch (Exception $e) {
      watchdog('riak_cache', $e, NULL, WATCHDOG_ERROR);
    }

    return empty($keys);
  }  
}
