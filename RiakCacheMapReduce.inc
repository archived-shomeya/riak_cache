<?php

class RiakCacheMapReduce extends RiakMapReduce {
  function run($timeout=NULL) {
    $num_phases = count($this->phases);
    $linkResultsFlag = FALSE;
    # If there are no phases, then just echo the inputs back to the user.
    if ($num_phases == 0) {
      $this->reduce(array("riak_kv_mapreduce", "reduce_identity"));
      $num_phases = 1;
      $linkResultsFlag = TRUE;
    }

    # Convert all phases to associative arrays. Also,
    # if none of the phases are accumulating, then set the last one to
    # accumulate.
    $keep_flag = FALSE;
    $query = array();
    for ($i = 0; $i < $num_phases; $i++) {
      $phase = $this->phases[$i];
      if ($i == ($num_phases - 1) && !$keep_flag)
        $phase->keep = TRUE;
      if ($phase->keep) $keep_flag = TRUE;
      $query[] = $phase->to_array();
    }

    # Construct the job, optionally set the timeout...
    $job = array("inputs"=>$this->inputs, "query"=>$query);
    if ($timeout != NULL) $job["timeout"] = $timeout;
    $content = json_encode($job);

    # Do the request...
    $url = "http://" . $this->client->host . ":" . $this->client->port . "/" . $this->client->mapred_prefix;
    $response = RiakUtils::httpRequest('POST', $url, array(), $content);
    $result = json_decode($response[1]);

    # If the last phase is NOT a link phase, then return the result.
    $linkResultsFlag |= (end($this->phases) instanceof RiakLinkPhase);

    # If we don't need to link results, then just return.
    if (!$linkResultsFlag) return $result;

    # Otherwise, if the last phase IS a link phase, then convert the
    # results to RiakLink objects.
    $a = array();
    foreach ($result as $r) {
      $link = new RiakLink($r[0], $r[1], $r[2]);
      $link->client = $this->client;
      $a[] = $link;
    }
    return $a;
  }
}
