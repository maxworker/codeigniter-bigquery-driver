<?php
defined('BASEPATH') or exit('No direct script access allowed');

require_once APPPATH . 'third_party/vendor/autoload.php';
require_once BASEPATH . 'database/DB_result.php';
if(file_exists(BASEPATH . 'database/drivers/bigquery/bigquery_result.php')) {
    require_once BASEPATH . 'database/drivers/bigquery/bigquery_result.php';
} else {
    require_once APPPATH . 'libraries/database/drivers/bigquery/bigquery_result.php';
}


# Imports the Google Cloud client library
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\Core\ExponentialBackoff;
use Google\Cloud\Core\Iterator\PageIterator;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\Core\Exception\GoogleException;

use PHPSQLParser\PHPSQLParser;
use PHPSQLParser\PHPSQLCreator;

class BigQueryResult
{
    protected $num_rows = 0;
    protected $num_fields = 0;
    protected $list_fields = [];
    protected $field_data = [];
    protected $result_data = [];

    protected $jobId = false;
    protected $nextPageToken = false;
    protected $startIndex = 0;
    protected $maxResults = 0; // all
    protected $errors = false;

    public function __construct(array $config = [])
    {
        $this->num_rows = array_key_exists('num_rows', $config) ? $config['num_rows'] : 0;
        $this->num_fields = array_key_exists('num_fields', $config) ? $config['num_fields'] : 0;
        $this->list_fields = array_key_exists('list_fields', $config) ? $config['list_fields'] : [];
        $this->field_data = array_key_exists('field_data', $config) ? $config['field_data'] : [];
        $this->result_data = array_key_exists('result_data', $config) ? $config['result_data'] : [];
        $this->jobId = array_key_exists('jobId', $config) ? $config['jobId'] : false;
        $this->nextPageToken = array_key_exists('nextPageToken', $config) ? $config['nextPageToken'] : false;
        $this->startIndex = array_key_exists('startIndex', $config) ? $config['startIndex'] : 0;
        $this->maxResults = array_key_exists('maxResults', $config) ? $config['maxResults'] : 0;
        $this->errors = array_key_exists('errors', $config) ? $config['errors'] : false;
    }

    public function num_rows()
    {
        return $this->num_rows;
    }

    public function num_fields()
    {
        return $this->num_fields;
    }

    public function list_fields()
    {
        return $this->list_fields;
    }

    public function field_data()
    {
        return $this->field_data;
    }

    public function result_data()
    {
        return $this->result_data;
    }

    public function jobId()
    {
        return $this->jobId;
    }

    public function nextPageToken()
    {
        return $this->nextPageToken;
    }

    public function startIndex()
    {
        return $this->startIndex;
    }

    public function maxResults()
    {
        return $this->maxResults;
    }

    public function errors()
    {
        return $this->errors;
    }
}

class BigQuery
{
    private $waitTimeDefaultForJob = 10;
    private $waitTimeForQuery = 5;

    /**
     * BigQuery Key Path
     *
     * @var string
     */
    private $keyFile;

    /**
     * BigQuery Project ID
     *
     * @var string
     */
    private $projectId;

    /**
     * BigQuery Dataset
     *
     * @var string
     */
    private $datasetName;

    protected $client;
    protected $tablesMetadata = [];

    protected $lastQuerySchema = [];
    protected $lastQueryTotalRows = 0;
    protected $lastQueryErrors = [];
    protected $lastJobId = '';

    public function __construct(array $config = [])
    {
        $this->keyFile = array_key_exists('BQ_KEY_FILE', $config) ? $config['BQ_KEY_FILE'] : "";
        $this->projectId = array_key_exists('BQ_PROJECT_ID', $config) ? $config['BQ_PROJECT_ID'] : "";
        $this->datasetName = array_key_exists('BQ_DATASET', $config) ? $config['BQ_DATASET'] : "";
    }

    /**
     * Get BigQuery API Client
     *
     * @return BigQueryClient BigQuery API Client
     */
    public function getClient()
    {
        if ($this->client) {
            return $this->client;
        }

        $keyFilePath = $this->keyFile;

        // Support relative and absolute path
        if (($keyFilePath[0] !== '/')&&($keyFilePath[1] !== ':')) {
            $keyFilePath = getcwd() . '/' . $keyFilePath;
        }

        if (! file_exists($keyFilePath)) {
            throw new \Exception('Google Service Account JSON Key File not found', 1);
        }

        return $this->client = new BigQueryClient([
            'projectId' => $this->projectId,
            'keyFile' => json_decode(file_get_contents($keyFilePath), true),
            'scopes' => [BigQueryClient::SCOPE]
        ]);
    }

    /**
     * Get table metadata
     * See https://cloud.google.com/bigquery/querying-data#metadata_about_tables_in_a_dataset
     *
     * @return array Array with all dataset tables information
     */
    public function getTablesMetadata()
    {
        $client = $this->getClient();
        $queryResults = $client->runQuery('SELECT * FROM ' . $this->datasetName . '.__TABLES__;');

        foreach ($queryResults->rows() as $row) {
            $this->tablesMetadata[$row['table_id']] = $row;
        }
        return $this->tablesMetadata;
    }

    /**
     * Check if a BigQuery table existis
     *
     * @param  string $tableName Table name
     * @return bool              True if table exists
     */
    public function tableExists($tableName)
    {
        $client = $this->getClient();
        $dataset = $client->dataset($this->datasetName);

        return $dataset->table($tableName)->exists();
    }

    /**
     * Get the number of rows on a table
     * @param  string $tableName Table name
     * @return int|bool          false if table doesn't exists, or the number of rows
     */
    public function getCountTableRows($tableName)
    {
        $this->getTablesMetadata();

        if (! array_key_exists($tableName, $this->tablesMetadata)) {
            return false;
        }

        return $this->tablesMetadata[$tableName]['row_count'];
    }

    /**
     * Run simple query
     * See https://cloud.google.com/bigquery/querying-data
     *
     * @return array Array
     */
    public function simpleQuery($sql)
    {
        $client = $this->getClient();
        $queryResults = $client->runQuery($sql);
        if ($queryResults->isComplete()) {
            $info = $queryResults->info();
            $this->lastQuerySchema = $info['schema']['fields'];
            $this->lastQueryTotalRows = $info['totalRows'];
            $this->lastQueryErrors = array_key_exists('errors', $info) ? $info['errors'] : [];
            return $queryResults->rows();
        } else {
            $this->lastQuerySchema = [];
            $this->lastQueryTotalRows = 0;
            $this->lastQueryErrors = [];
            return false;
        }

    }

    /**
     * Run a BigQuery query as a job.
     * Example:
     * ```
     * $query = 'SELECT TOP(corpus, 10) as title, COUNT(*) as unique_words ' .
     *          'FROM [publicdata:samples.shakespeare]';
     * run_query_as_job($projectId, $query, true);
     * ```.
     *
     * @param string $query     A SQL query to run. *
     * @param bool $useLegacySql Specifies whether to use BigQuery's legacy SQL
     *        syntax or standard SQL syntax for this query.
     */
    public function runQueryJob($query, $useLegacySql = false)
    {
        $bigQuery = $this->getClient();

        $job = $bigQuery->runQueryAsJob(
            $query,
            ['jobConfig' => ['useLegacySql' => $useLegacySql]]
        );

        $backoff = new ExponentialBackoff($this->waitTimeDefaultForJob);
        $backoff->execute(function () use ($job) {
            $job->reload();
            return $job->isComplete();
        });
        $queryResults = $job->queryResults();

        if ($queryResults->isComplete()) {
            $info = $queryResults->info();
            $this->lastQuerySchema = $info['schema']['fields'];
            $this->lastQueryTotalRows = $info['totalRows'];
            $this->lastQueryErrors = array_key_exists('errors', $info) ? $info['errors'] : [];
            //$this->lastJobId = $job->id();
            return $queryResults->rows();
        } else {
            $info = $queryResults->info();
            $this->lastQuerySchema = [];
            $this->lastQueryTotalRows = 0;
            $this->lastQueryErrors = array_key_exists('errors', $info) ? $info['errors'] : [];
            //$this->lastJobId = $job->id();
            return false;
        }
    }

    public function queryJob($query, $useLegacySql = false)
    {
        $options = ['jobConfig' => ['useLegacySql' => $useLegacySql]];
        $bigQuery = $this->getClient();
        $job = $bigQuery->runQueryAsJob(
            $query,
            $options
        );
        $this->lastJobId = $job->id();
        return $this->lastJobId;
    }

    public function connectToJob($jobId)
    {
        $bigQuery = $this->getClient();
        $this->lastJobId = $jobId;
        return $bigQuery->job($jobId);
    }

    public function waitForJobComplete($job, $waitTime = 10)
    {
        $backoff = new ExponentialBackoff($waitTime);
        $backoff->execute(function () use ($job) {
            $job->reload();
            return $job->isComplete();
        });
        return true;
    }

    public function viewTable($tableName, $maxResults = 10, $startIndex = 0)
    {
        $bigQuery = $this->getClient();
        $options = [
            'maxResults' => $maxResults,
            'startIndex' => $startIndex
        ];
        $dataset = $bigQuery->dataset($this->datasetName);
        $table = $dataset->table($tableName);
        return $table->rows($options);
    }

    public function getAllFromJobResult($job)
    {
        $info = $job->queryResults()->info();
        $this->lastQuerySchema = $info['schema']['fields'];
        $this->lastQueryTotalRows = $info['totalRows'];
        $this->lastQueryErrors = array_key_exists('errors', $info) ? $info['errors'] : [];
        return $job->queryResults()->rows();
    }

    public function getSchemaFromJob($job)
    {
        $info = $job->queryResults()->info();
        $this->lastQuerySchema = $info['schema']['fields'];
        $this->lastQueryTotalRows = $info['totalRows'];
        $this->lastQueryErrors = array_key_exists('errors', $info) ? $info['errors'] : [];
        return $this->lastQuerySchema;
    }

    public function getPageFromJobResultByToken($job, $maxResults = 10, $pageToken = null)
    {
        $options = [
            'maxResults' => $maxResults,
            'pageToken' => $pageToken
        ];
        $info = $job->queryResults()->info();
        if (count($this->lastQuerySchema) == 0) {
            $this->lastQuerySchema = $info['schema']['fields'];
        }
        $this->lastQueryTotalRows = $info['totalRows'];
        return $this->pageQueryResultsByToken($job->queryResults($options), $options);
    }

    public function getPageFromJobResultByIndex($job, $maxResults = 10, $startIndex = 0)
    {
        $options = [
            'maxResults' => $maxResults,
            'startIndex' => $startIndex
        ];
        $info = $job->queryResults()->info();
        if (count($this->lastQuerySchema) == 0) {
            $this->lastQuerySchema = $info['schema']['fields'];
        }
        $this->lastQueryTotalRows = $info['totalRows'];
        return $this->pageQueryResultsByIndex($job->queryResults($options), $options);
    }

    public function dumpResults($rows)
    {
        $result = "";
        $i = 0;
        foreach ($rows as $row) {
            $result .= sprintf('--- Row %s ---' . PHP_EOL, ++$i);
            foreach ($row as $column => $value) {
                $result .= sprintf('%s: %s' . PHP_EOL, $column, $value);
            }
        }
        $result .= printf('Found %s row(s)' . PHP_EOL, $i);
        return $result;
    }

    private function pageQueryResultsByToken(QueryResults $results, array $options = [])
    {
        if (!$results->isComplete()) {
            //throw new GoogleException('The query has not completed yet.');
            return false;
        }

        $schema = $results->info()['schema']['fields'];

        $resultsProperties = Closure::bind(function($prop){return $this->$prop;}, $results, $results);

        return new PageIterator(
            function (array $row) use ($schema, $results, $resultsProperties) {
                $mergedRow = [];

                if ($row === null) {
                    return $mergedRow;
                }

                if (!array_key_exists('f', $row)) {
                    return false;
                }

                foreach ($row['f'] as $key => $value) {
                    $fieldSchema = $schema[$key];
                    $mergedRow[$fieldSchema['name']] = $resultsProperties('mapper')->fromBigQuery($value, $fieldSchema);
                }

                return $mergedRow;
            },
            [$resultsProperties('connection'), 'getQueryResults'],
            $options + $results->identity(),
            [
                'itemsKey' => 'rows',
                'firstPage' => $results->info(),
                'nextResultTokenKey' => 'pageToken'
            ]
        );
    }

    private function pageQueryResultsByIndex(QueryResults $results, array $options = [])
    {
        if (!$results->isComplete()) {
            //throw new GoogleException('The query has not completed yet.');
            return false;
        }

        $schema = $results->info()['schema']['fields'];

        $resultsProperties = Closure::bind(  function($prop){return $this->$prop;}, $results, $results);

        return new PageIterator(
            function (array $row) use ($schema, $results, $resultsProperties) {
                $mergedRow = [];

                if ($row === null) {
                    return $mergedRow;
                }

                if (!array_key_exists('f', $row)) {
                    //throw new GoogleException('Bad response - missing key "f" for a row.');
                    return false;
                }

                foreach ($row['f'] as $key => $value) {
                    $fieldSchema = $schema[$key];
                    $mergedRow[$fieldSchema['name']] = $resultsProperties('mapper')->fromBigQuery($value, $fieldSchema);
                }

                return $mergedRow;
            },
            [$resultsProperties('connection'), 'getQueryResults'],
            $options + $results->identity(),
            [
                'itemsKey' => 'rows',
                'firstPage' => $results->info()
            ]
        );
    }

    public function getQuerySchema()
    {
        return $this->lastQuerySchema;
    }

    public function getQueryTotalRows()
    {
        //log_message("error", "BQ: ". $this->lastQueryTotalRows);
        return intval($this->lastQueryTotalRows);
    }

    public function getQueryErrors()
    {
        return $this->lastQueryErrors;
    }

    public function getJobId()
    {
        return $this->lastJobId;
    }

    public function query($sql)
    {
        $queryObject = json_decode($sql);

        if (json_last_error() === JSON_ERROR_NONE) {
            // its query object with Job Id
            //$queryObject->startIndex
            //$queryObject->jobId
            //$queryObject->maxResults

            $job = $this->connectToJob($queryObject->jobId);
            $this->jobId = $queryObject->jobId;

            if ($this->waitForJobComplete($job, 1) !== false) {
                // check for errors
                if (count($this->lastQueryErrors) > 0) {
                    return new BigQueryResult([
                        "errors"=> true,
                        "jobId"=>$this->jobId,
                        "startIndex"=>$queryObject->startIndex,
                        "maxResults"=>$queryObject->maxResults
                    ]);
                }

                if ($queryObject->maxResults !== 0) {
                    // has pagination
                    $page = $this->getPageFromJobResultByIndex($job, $queryObject->maxResults, $queryObject->startIndex);
                    return new BigQueryResult(["jobId"=>$this->jobId,
                                                 "startIndex"=> $queryObject->startIndex,
                                                 "maxResults"=> $queryObject->maxResults,
                                                 "num_rows" => $this->lastQueryTotalRows,
                                                 "num_fields" => count($this->lastQuerySchema),
                                                 "list_fields" => count($this->lastQuerySchema) ? array_column($this->lastQuerySchema, 'name') : [],
                                                 "field_data" => $this->lastQuerySchema,
                                                 "result_data" => $page // ->current()
                                                 ]);
                } else {
                    $data = $this->getAllFromJobResult($job);
                    return new BigQueryResult(["jobId"=>$this->jobId,
                                                 "startIndex"=> 0,
                                                 "maxResults"=> 0,
                                                 "num_rows" => $this->lastQueryTotalRows,
                                                 "num_fields" => count($this->lastQuerySchema),
                                                 "list_fields" => count($this->lastQuerySchema) ? array_column($this->lastQuerySchema, 'name') : [],
                                                 "field_data" => $this->lastQuerySchema,
                                                 "result_data" => $data
                                                 ]);
                }
            } else {
                // check for errors
                if (count($this->lastQueryErrors) > 0) {
                    return new BigQueryResult([
                        "errors"=> true,
                        "jobId"=>$this->jobId,
                        "startIndex"=>$queryObject->startIndex,
                        "maxResults"=>$queryObject->maxResults
                    ]);
                }
                // return the same
                return new BigQueryResult([
                    "jobId"=>$this->jobId,
                    "startIndex"=>$queryObject->startIndex,
                    "maxResults"=>$queryObject->maxResults
                ]);
            }
        } else {
            //determine limit options
            $parser = new PHPSQLParser();
            $parsed = $parser->parse($sql);
            $startIndex = false;
            $maxResults = false;
            if (array_key_exists("LIMIT", $parsed)) {
                $startIndex = $parsed["LIMIT"]["offset"];
                $maxResults = $parsed["LIMIT"]["rowcount"];
            }

            if (($startIndex !== false)&&($startIndex !== '')) {
                // none supported limit present
                unset($parsed["LIMIT"]);
                $creator = new PHPSQLCreator($parsed);
                $sql =  $creator->created;
            }

            // its query string
            $jobId = $this->queryJob($sql);
            $job = $this->connectToJob($jobId);
            if ($this->waitForJobComplete($job, $this->waitTimeForQuery) !== false) {
                // check for errors
                if (count($this->lastQueryErrors) > 0) {
                    return new BigQueryResult([
                        "errors"=> true,
                        "jobId"=>$jobId,
                        "startIndex"=>$startIndex,
                        "maxResults"=>$maxResults
                    ]);
                }

                // wait 5 sec, if success then return dataset
                if (($startIndex !== false)&&($startIndex !== '')) {
                    // get page with limit
                    $page = $this->getPageFromJobResultByIndex($job, $maxResults, $startIndex);

                    return new BigQueryResult([
                        "jobId"=>$jobId,
                        "startIndex"=> $startIndex,
                        "maxResults"=> $maxResults,
                        "num_rows" => $this->lastQueryTotalRows,
                        "num_fields" => count($this->lastQuerySchema),
                        "list_fields" => count($this->lastQuerySchema) ? array_column($this->lastQuerySchema, 'name') : [],
                        "field_data" => $this->lastQuerySchema,
                        "result_data" => $page //->current()
                    ]);
                } else {
                    $data = $this->getAllFromJobResult($job);
                    return new BigQueryResult([
                        "jobId"=>$jobId,
                        "startIndex"=> 0,
                        "maxResults"=> 0,
                        "num_rows" => $this->lastQueryTotalRows,
                        "num_fields" => count($this->lastQuerySchema),
                        "list_fields" => count($this->lastQuerySchema) ? array_column($this->lastQuerySchema, 'name') : [],
                        "field_data" => $this->lastQuerySchema,
                        "result_data" => $data
                    ]);
                }
            } else {
                // if not results, then return JobID
                if ($startIndex === false) {
                    $startIndex = 0;
                }
                if ($maxResults === false) {
                    $maxResults = 0;
                }

                // check for errors
                if (count($this->lastQueryErrors) > 0) {
                    return new BigQueryResult([
                        "errors"=> true,
                        "jobId"=>$jobId,
                        "startIndex"=>$startIndex,
                        "maxResults"=>$maxResults
                    ]);
                }

                return new BigQueryResult([
                    "jobId"=>$jobId,
                    "startIndex"=>$startIndex,
                    "maxResults"=>$maxResults
                ]);
            }
        }
    }
}
