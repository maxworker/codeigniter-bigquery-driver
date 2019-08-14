<?php
defined('BASEPATH') or exit('No direct script access allowed');

class Test_bq_model extends CI_Model
{

    public $db = null; // overrride

    public function __construct()
    {
        parent::__construct();
        $this->load->helper('DynamicDb');
    }

    public function loadDatabase()
    {
        $db_params = array(
            'dsn'   => '',
            'hostname' => 'sample-engine', // name of engine
            'username' => "gkey.json", // key file
            'password' => '',
            'database' => 'sample_dataset', // dataset name
            'dbdriver' => 'bigquery',
            'dbprefix' => '',
            'pconnect' => false,
            'db_debug' => false,
            'cache_on' => false,
            'cachedir' => APPPATH.'cache/dbcache/bq/',
            'char_set' => 'utf8',
            'dbcollat' => 'utf8_general_ci',
            'swap_pre' => '',
            'encrypt' => false,
            'compress' => false,
            'stricton' => false,
            'failover' => array(),
            'save_queries' => false
        );

        try {
            $this->db = dynamicDb($db_params);
            return !empty($this->db->conn_id);
        } catch (Exception $e) {
            return false;
        }
    }

    public function getData($sql)
    {
        $queryResult = $this->db->query($sql);
        return $queryResult->result_array();
    }

}
