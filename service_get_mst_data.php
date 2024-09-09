<?php 
ini_set('post_max_size', '264M');
ini_set('upload_max_filesize', '264M');
ini_set('memory_limit', '296M');
ini_set('max_execution_time', 3000);

/* config mssql */
// $server = '10.0.11.142';
$server = '10.0.89.142';
$username = 'mitusr';  
$password = 'WOM@2022';
$con = mssql_connect($server, $username, $password);
mssql_select_db( "WISE_STAGING", $con );


/* config mysql */ 
$conf_ip            = "localhost";  
$conf_user          = "es";
$conf_passwd        = "0218Galunggung";
$conf_db            = "db_wom";


function connectDB() {
    global $conf_ip, $conf_user, $conf_passwd, $conf_db ;   
    if (!$connect=mysqli_connect($conf_ip, $conf_user, $conf_passwd, $conf_db)) {
      $filename = __FILE__;
      $linename = __LINE__;
     // exit();
    }
    return $connect;
}


function disconnectDB($db_connect) {
    mysqli_close($db_connect);
}


if ($con) {
    //echo "Koneksi Berhasil !";
} else {
    echo "Koneksi gagal !";
    die(print_r(mssql_error(),true));
}

echo "Process...";
echo "<br>";
echo "<br>";

$dbopen  = connectDB();
$datenow = DATE("Y-m-d");
// 1
$mss_1 = "SELECT * FROM WISE_STAGING..STG_M_CC_CRM_OFFICE_REGION_MBR_X (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
  $OFFICE_REGION_MBR_X_ID   = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_REGION_MBR_X_ID']);
  $OFFICE_REGION_X_ID       = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_REGION_X_ID']);
  $REF_OFFICE_AREA_ID       = mysqli_real_escape_string($dbopen,$rcs_1['REF_OFFICE_AREA_ID']);
  $USR_CRT                  = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
  $DTM_CRT                  = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
  $DTM_CRT                  = date("Y-m-d h:i:s", strtotime($DTM_CRT));
  $USR_UPD                  = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
  $DTM_UPD                  = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
  $DTM_UPD                  = date("Y-m-d h:i:s", strtotime($DTM_UPD));


   $no =1;
   $sqlin = "INSERT INTO cc_master_mapping SET 
                region_mbr_id       ='$OFFICE_REGION_MBR_X_ID',    
                region_id           ='$OFFICE_REGION_X_ID',
                area_id             ='$REF_OFFICE_AREA_ID',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD' 
              ON DUPLICATE KEY UPDATE 
                region_mbr_id       ='$OFFICE_REGION_MBR_X_ID',    
                region_id           ='$OFFICE_REGION_X_ID',
                area_id             ='$REF_OFFICE_AREA_ID',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);

        $suc1 += $no;
    }else{
        $err1 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_OFFICE_REGION_MBR_X',
                        sync_error  ='$OFFICE_REGION_X_ID',
                        sync_time   =now()";
        mysqli_query($dbopen,$sqlerrs);
    }
    

}

//log 
 $sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_OFFICE_REGION_MBR_X',
                sync_success    ='$suc1',
                sync_error      ='$err1',
                sync_time       =now()";
 mysqli_query($dbopen,$sqllog);


 echo "Sync Data STG_M_CC_CRM_OFFICE_REGION_MBR_X OK <br>";

// 2
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REF_OFFICE_AREA (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){

  $REF_OFFICE_AREA_ID   = mysqli_real_escape_string($dbopen,$rcs_1['REF_OFFICE_AREA_ID']);
  $AREA_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['AREA_CODE']);
  $AREA_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['AREA_NAME']);
  $IS_ACTIVE            = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
  $USR_CRT              = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
  $DTM_CRT              = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
  $USR_UPD              = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
  $USR_UPD              = date("Y-m-d h:i:s", strtotime($USR_UPD));
  $DTM_UPD              = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
  $DTM_UPD              = date("Y-m-d h:i:s", strtotime($DTM_UPD));
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_area SET 
                area_id     ='$REF_OFFICE_AREA_ID',
                area_code   ='$AREA_CODE',
                area_name   ='$AREA_NAME',
                is_active   ='$IS_ACTIVE',
                usr_crt     ='$USR_CRT',
                dtm_crt     ='$DTM_CRT',
                usr_upd     ='$USR_UPD',
                dtm_upd     ='$DTM_UPD'
            ON DUPLICATE KEY UPDATE
                area_id     ='$REF_OFFICE_AREA_ID',
                area_code   ='$AREA_CODE',
                area_name   ='$AREA_NAME',
                is_active   ='$IS_ACTIVE',
                usr_crt     ='$USR_CRT',
                dtm_crt     ='$DTM_CRT',
                usr_upd     ='$USR_UPD',
                dtm_upd     ='$DTM_UPD'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc2 += $no;
    }else{
        $err2 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_REF_OFFICE_AREA',
                        sync_error  ='$AREA_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_REF_OFFICE_AREA',
            sync_success    ='$suc2',
            sync_error      ='$err2',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_OFFICE_AREA OK <br>";

// 3
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REF_OFFICE (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $REF_OFFICE_ID          = mysqli_real_escape_string($dbopen,$rcs_1['REF_OFFICE_ID']);
        $OFFICE_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_CODE']);
        $OFFICE_SHORT_NAME      = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_SHORT_NAME']);
        $OFFICE_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_NAME']);
        $ORG_MDL_ID             = mysqli_real_escape_string($dbopen,$rcs_1['ORG_MDL_ID']);
        $OFFICE_ADDR            = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_ADDR']);
        $RT                     = mysqli_real_escape_string($dbopen,$rcs_1['RT']);
        $RW                     = mysqli_real_escape_string($dbopen,$rcs_1['RW']);
        $KELURAHAN              = mysqli_real_escape_string($dbopen,$rcs_1['KELURAHAN']);
        $KECAMATAN              = mysqli_real_escape_string($dbopen,$rcs_1['KECAMATAN']);
        $CITY                   = mysqli_real_escape_string($dbopen,$rcs_1['CITY']);
        $ZIPCODE                = mysqli_real_escape_string($dbopen,$rcs_1['ZIPCODE']);
        $PHN_AREA_1             = mysqli_real_escape_string($dbopen,$rcs_1['PHN_AREA_1']);
        $PHN_1                  = mysqli_real_escape_string($dbopen,$rcs_1['PHN_1']);
        $PHN_AREA_2             = mysqli_real_escape_string($dbopen,$rcs_1['PHN_AREA_2']);
        $PHN_2                  = mysqli_real_escape_string($dbopen,$rcs_1['PHN_2']);
        $FAX_AREA               = mysqli_real_escape_string($dbopen,$rcs_1['FAX_AREA']);
        $FAX                    = mysqli_real_escape_string($dbopen,$rcs_1['FAX']);
        $CNTCT_PERSON_NAME      = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_NAME']);
        $CNTCT_PERSON_JOB_TITLE = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_JOB_TITLE']);
        $CNTCT_PERSON_EMAIL_1   = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_EMAIL_1']);
        $CNTCT_PERSON_MOBILE_PHN_1  = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_MOBILE_PHN_1']);
        $CNTCT_PERSON_MOBILE_PHN_2  = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_MOBILE_PHN_2']);
        $REF_OFFICE_AREA_ID         = mysqli_real_escape_string($dbopen,$rcs_1['REF_OFFICE_AREA_ID']);
        $IS_ACTIVE              = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $PARENT_ID              = mysqli_real_escape_string($dbopen,$rcs_1['PARENT_ID']);
        $IS_OFFICE_CLOSE        = mysqli_real_escape_string($dbopen,$rcs_1['IS_OFFICE_CLOSE']);
        $USR_UPD                = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
        $USR_CRT                = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_cabang SET 
                office_id               ='$REF_OFFICE_ID', 
                office_code             ='$OFFICE_CODE',
                office_short_name       ='$OFFICE_SHORT_NAME',
                office_name             ='$OFFICE_NAME',
                org_mdl_id              ='$ORG_MDL_ID',
                office_address          ='$OFFICE_ADDR',
                rt                      ='$RT',
                rw                      ='$RW',
                kelurahan               ='$KELURAHAN',
                kecamatan               ='$KECAMATAN',
                city                    ='$CITY',
                zipcode                 ='$ZIPCODE',
                phn_area_1              ='$PHN_AREA_1',
                phn_1                   ='$PHN_1',
                phn_area_2              ='$PHN_AREA_2',
                phn_2                   ='$PHN_2',
                phn_area_3              ='',
                phn_3                   ='',
                fax_area                ='$FAX_AREA',
                fax                     ='$FAX',
                cntc_person_name        ='$CNTCT_PERSON_NAME',
                cntc_person_job_title   ='$CNTCT_PERSON_JOB_TITLE',
                cntc_person_email_1     ='$CNTCT_PERSON_EMAIL_1',
                cntc_person_email_2     ='',
                cntc_person_mobile_phn_1='$CNTCT_PERSON_MOBILE_PHN_1',
                cntc_person_mobile_phn_2='$CNTCT_PERSON_MOBILE_PHN_2',
                mr_office_class         ='',
                ref_office_area_id      ='$REF_OFFICE_AREA_ID',
                is_active               ='$IS_ACTIVE',
                parent_id               ='$PARENT_ID',
                is_office_close         ='$IS_OFFICE_CLOSE',
                usr_crt                 ='$USR_UPD',
                dtm_crt                 ='$DTM_UPD',
                usr_upd                 ='$USR_CRT',
                dtm_upd                 ='$DTM_CRT'
             ON DUPLICATE KEY UPDATE 
                office_id               ='$REF_OFFICE_ID', 
                office_code             ='$OFFICE_CODE',
                office_short_name       ='$OFFICE_SHORT_NAME',
                office_name             ='$OFFICE_NAME',
                org_mdl_id              ='$ORG_MDL_ID',
                office_address          ='$OFFICE_ADDR',
                rt                      ='$RT',
                rw                      ='$RW',
                kelurahan               ='$KELURAHAN',
                kecamatan               ='$KECAMATAN',
                city                    ='$CITY',
                zipcode                 ='$ZIPCODE',
                phn_area_1              ='$PHN_AREA_1',
                phn_1                   ='$PHN_1',
                phn_area_2              ='$PHN_AREA_2',
                phn_2                   ='$PHN_2',
                phn_area_3              ='',
                phn_3                   ='',
                fax_area                ='$FAX_AREA',
                fax                     ='$FAX',
                cntc_person_name        ='$CNTCT_PERSON_NAME',
                cntc_person_job_title   ='$CNTCT_PERSON_JOB_TITLE',
                cntc_person_email_1     ='$CNTCT_PERSON_EMAIL_1',
                cntc_person_email_2     ='',
                cntc_person_mobile_phn_1='$CNTCT_PERSON_MOBILE_PHN_1',
                cntc_person_mobile_phn_2='$CNTCT_PERSON_MOBILE_PHN_2',
                mr_office_class         ='',
                ref_office_area_id      ='$REF_OFFICE_AREA_ID',
                is_active               ='$IS_ACTIVE',
                parent_id               ='$PARENT_ID',
                is_office_close         ='$IS_OFFICE_CLOSE',
                usr_crt                 ='$USR_UPD',
                dtm_crt                 ='$DTM_UPD',
                usr_upd                 ='$USR_CRT',
                dtm_upd                 ='$DTM_CRT' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        
        $suc3 += $no;
    }else{
        $err3 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_REF_OFFICE',
                        sync_error  ='$OFFICE_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_REF_OFFICE',
                sync_success    ='$suc3',
                sync_error      ='$err3',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_OFFICE OK <br>";


// 4
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_PROD (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $PROD_ID                    = mysqli_real_escape_string($dbopen,$rcs_1['PROD_ID']);
        $PRODUCT_CODE               = mysqli_real_escape_string($dbopen,$rcs_1['PRODUCT_CODE']);
        $PRODUCT_NAME               = mysqli_real_escape_string($dbopen,$rcs_1['PRODUCT_NAME']);
        $CURRENT_PRODUCT_H_ID       = mysqli_real_escape_string($dbopen,$rcs_1['CURRENT_PRODUCT_H_ID']);
        $DRAFT_PRODUCT_H_ID         = mysqli_real_escape_string($dbopen,$rcs_1['DRAFT_PRODUCT_H_ID']);
        $START_DT                   = mysqli_real_escape_string($dbopen,$rcs_1['START_DT']);
        $EXPIRED_DT                 = mysqli_real_escape_string($dbopen,$rcs_1['EXPIRED_DT']);
        $IS_ACTIVE                  = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $USR_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_product SET 
                product_id              ='$PROD_ID',
                product_code            ='$PRODUCT_CODE',
                product_name            ='$PRODUCT_NAME',
                current_product_h_id    ='$CURRENT_PRODUCT_H_ID',
                draft_product_h_id      ='$DRAFT_PRODUCT_H_ID',
                start_date              ='$START_DT',
                expired_date            ='$EXPIRED_DT',
                is_active               ='$IS_ACTIVE',
                is_centralized          ='',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'
             ON DUPLICATE KEY UPDATE 
                product_id              ='$PROD_ID',
                product_code            ='$PRODUCT_CODE',
                product_name            ='$PRODUCT_NAME',
                current_product_h_id    ='$CURRENT_PRODUCT_H_ID',
                draft_product_h_id      ='$DRAFT_PRODUCT_H_ID',
                start_date              ='$START_DT',
                expired_date            ='$EXPIRED_DT',
                is_active               ='$IS_ACTIVE',
                is_centralized          ='',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc4 += $no;
    }else{
        $err4 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_PROD',
                    sync_error  ='$PRODUCT_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_PROD',
            sync_success    ='$suc4',
            sync_error      ='$err4',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_PROD OK <br>";



// 5
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_ASSET_TYPE (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $ASSET_TYPE_ID              = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_TYPE_ID']);
        $ASSET_TYPE_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_TYPE_CODE']);
        $ASSET_TYPE_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_TYPE_NAME']);
        $SERIAL_NO_2_LABEL          = mysqli_real_escape_string($dbopen,$rcs_1['SERIAL_NO_2_LABEL']);
        $SERIAL_NO_1_LABEL          = mysqli_real_escape_string($dbopen,$rcs_1['SERIAL_NO_1_LABEL']);
        $HIERARCHY_LEVEL_1_LABEL    = mysqli_real_escape_string($dbopen,$rcs_1['HIERARCHY_LEVEL_1_LABEL']);
        $HIERARCHY_LEVEL_2_LABEL    = mysqli_real_escape_string($dbopen,$rcs_1['HIERARCHY_LEVEL_2_LABEL']);
        $HIERARCHY_LEVEL_3_LABEL    = mysqli_real_escape_string($dbopen,$rcs_1['HIERARCHY_LEVEL_3_LABEL']);
        $SANDI_BI_CF                = mysqli_real_escape_string($dbopen,$rcs_1['SANDI_BI_CF']);
        $SANDI_BI_LS                = mysqli_real_escape_string($dbopen,$rcs_1['SANDI_BI_LS']);
        $USR_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_type_asset SET 
                asset_type_id               ='$ASSET_TYPE_ID',
                asset_type_code             ='$ASSET_TYPE_CODE',
                asset_type_name             ='$ASSET_TYPE_NAME',
                serial_no_1_label           ='$SERIAL_NO_1_LABEL',
                serial_no_2_label           ='$SERIAL_NO_2_LABEL',
                hierarchy_level_1_label     ='$HIERARCHY_LEVEL_1_LABEL',
                hierarchy_level_2_label     ='$HIERARCHY_LEVEL_2_LABEL',
                hierarchy_level_3_label     ='$HIERARCHY_LEVEL_3_LABEL',
                sandi_bl_cf                 ='$SANDI_BI_CF',
                sandi_bl_ls                 ='$SANDI_BI_LS',    
                usr_crt                     ='$USR_CRT',
                dtm_crt                     ='$DTM_CRT',
                usr_upd                     ='$USR_UPD',
                dtm_upd                     ='$DTM_UPD'
             ON DUPLICATE KEY UPDATE 
                asset_type_id               ='$ASSET_TYPE_ID',
                asset_type_code             ='$ASSET_TYPE_CODE',
                asset_type_name             ='$ASSET_TYPE_NAME',
                serial_no_1_label           ='$SERIAL_NO_1_LABEL',
                serial_no_2_label           ='$SERIAL_NO_2_LABEL',
                hierarchy_level_1_label     ='$HIERARCHY_LEVEL_1_LABEL',
                hierarchy_level_2_label     ='$HIERARCHY_LEVEL_2_LABEL',
                hierarchy_level_3_label     ='$HIERARCHY_LEVEL_3_LABEL',
                sandi_bl_cf                 ='$SANDI_BI_CF',
                sandi_bl_ls                 ='$SANDI_BI_LS',    
                usr_crt                     ='$USR_CRT',
                dtm_crt                     ='$DTM_CRT',
                usr_upd                     ='$USR_UPD',
                dtm_upd                     ='$DTM_UPD'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc5 += $no;
    }else{
        $err5 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_ASSET_TYPE',
                    sync_error  ='$ASSET_TYPE_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_ASSET_TYPE',
                sync_success    ='$suc5',
                sync_error      ='$err5',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_ASSET_TYPE OK <br>";

// 6
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_ASSET_MASTER (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $ASSET_MASTER_ID        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_MASTER_ID']);
        $ASSET_CODE             = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_CODE']);
        $ASSET_NAME             = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_NAME']);
        $IS_ACTIVE              = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $ASSET_CATEGORY_ID      = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_CATEGORY_ID']);
        $ASSET_HIERARCHY_L1_ID  = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L1_ID']);
        $ASSET_HIERARCHY_L2_ID  = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L2_ID']);
        $USR_CRT                = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
        $ASSET_MASTER_NAME      = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_MASTER_NAME']);
        $ASSET_FULL_NAME        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_FULL_NAME']);
        $FLAG                   = mysqli_real_escape_string($dbopen,$rcs_1['FLAG']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_kendaraan SET 
                asset_id                ='$ASSET_MASTER_ID',
                asset_code              ='$ASSET_CODE',
                asset_name              ='$ASSET_NAME',
                asset_master_name       ='$ASSET_MASTER_NAME',
                asset_full_name         ='$ASSET_FULL_NAME',
                asset_category_id       ='$ASSET_CATEGORY_ID',
                asset_hierarchy_l1_id   ='$ASSET_HIERARCHY_L1_ID',
                asset_hierarchy_l2_id   ='$ASSET_HIERARCHY_L2_ID',
                is_active               ='$IS_ACTIVE',
                flag                    ='$FLAG',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'
              ON DUPLICATE KEY UPDATE 
                asset_id                ='$ASSET_MASTER_ID',
                asset_code              ='$ASSET_CODE',
                asset_name              ='$ASSET_NAME',
                asset_master_name       ='$ASSET_MASTER_NAME',
                asset_full_name         ='$ASSET_FULL_NAME',
                asset_category_id       ='$ASSET_CATEGORY_ID',
                asset_hierarchy_l1_id   ='$ASSET_HIERARCHY_L1_ID',
                asset_hierarchy_l2_id   ='$ASSET_HIERARCHY_L2_ID',
                is_active               ='$IS_ACTIVE',
                flag                    ='$FLAG',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc6 += $no;
    }else{
        $err6 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_ASSET_MASTER',
                    sync_error  ='$ASSET_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_ASSET_MASTER',
                sync_success    ='$suc6',
                sync_error      ='$err6',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_ASSET_MASTER OK <br>";


// 7
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_ASSET_HIERARCHY_L1 (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $ASSET_HIERARCHY_L1_ID          = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L1_ID']);
        $ASSET_HIERARCHY_L1_CODE        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L1_CODE']);
        $ASSET_TYPE_ID                  = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_TYPE_ID']);
        $ASSET_HIERARCHY_L1_NAME        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L1_NAME']);
        $IS_ACTIVE                      = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $USR_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
       
   $no =1;
   $sqlin = "INSERT INTO cc_master_merk SET 
                asset_hierarchy_l1_id   ='$ASSET_HIERARCHY_L1_ID',
                asset_hierarchy_l1_code ='$ASSET_HIERARCHY_L1_CODE',
                asset_hierarchy_l1_name ='$ASSET_HIERARCHY_L1_NAME',
                asset_type_id           ='$ASSET_TYPE_ID',
                is_active               ='$IS_ACTIVE',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'
            ON DUPLICATE KEY UPDATE
                asset_hierarchy_l1_id   ='$ASSET_HIERARCHY_L1_ID',
                asset_hierarchy_l1_code ='$ASSET_HIERARCHY_L1_CODE',
                asset_hierarchy_l1_name ='$ASSET_HIERARCHY_L1_NAME',
                asset_type_id           ='$ASSET_TYPE_ID',
                is_active               ='$IS_ACTIVE',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'  ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc7 += $no;
    }else{
        $err7 += $no;

           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_ASSET_HIERARCHY_L1',
                        sync_error  ='$ASSET_HIERARCHY_L1_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_ASSET_HIERARCHY_L1',
                sync_success    ='$suc7',
                sync_error      ='$err7',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_ASSET_HIERARCHY_L1 OK <br>";

// 8
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_ASSET_HIERARCHY_L2 (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $ASSET_HIERARCHY_L2_ID          = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L2_ID']);
        $ASSET_HIERARCHY_L2_CODE        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L2_CODE']);
        $ASSET_HIERARCHY_L2_NAME        = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L2_NAME']);
        $ASSET_HIERARCHY_L1_ID          = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_HIERARCHY_L1_ID']);
        $IS_ACTIVE                      = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $USR_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
       
   $no =1;
   $sqlin = "INSERT INTO cc_master_merk_group SET 
                asset_hierarchy_l2_id       ='$ASSET_HIERARCHY_L2_ID',
                asset_hierarchy_l2_code     ='$ASSET_HIERARCHY_L2_CODE',
                asset_hierarchy_l2_name     ='$ASSET_HIERARCHY_L2_NAME',
                asset_hierarchy_l1_id       ='$ASSET_HIERARCHY_L1_ID',        
                is_active                   ='$IS_ACTIVE',
                usr_crt                     ='$USR_CRT',
                dtm_crt                     ='$DTM_CRT',
                usr_upd                     ='$USR_UPD',
                dtm_upd                     ='$DTM_UPD'
             ON DUPLICATE KEY UPDATE
                asset_hierarchy_l2_id       ='$ASSET_HIERARCHY_L2_ID',
                asset_hierarchy_l2_code     ='$ASSET_HIERARCHY_L2_CODE',
                asset_hierarchy_l2_name     ='$ASSET_HIERARCHY_L2_NAME',
                asset_hierarchy_l1_id       ='$ASSET_HIERARCHY_L1_ID',        
                is_active                   ='$IS_ACTIVE',
                usr_crt                     ='$USR_CRT',
                dtm_crt                     ='$DTM_CRT',
                usr_upd                     ='$USR_UPD',
                dtm_upd                     ='$DTM_UPD'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc8 += $no;
    }else{
        $err8 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_ASSET_HIERARCHY_L2',
                    sync_error  ='$ASSET_HIERARCHY_L2_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_ASSET_HIERARCHY_L2',
            sync_success    ='$suc8',
            sync_error      ='$err8',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_ASSET_HIERARCHY_L2 OK <br>";


// 9
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_ASSET_CATEGORY (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $ASSET_CATEGORY_ID              = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_CATEGORY_ID']);
        $ASSET_CATEGORY_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_CATEGORY_CODE']);
        $ASSET_TYPE_ID                  = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_TYPE_ID']);
        $ASSET_CATEGORY_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['ASSET_CATEGORY_NAME']);
        $IS_ACTIVE                      = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $USR_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
       
   $no =1;
   $sqlin = "INSERT INTO cc_master_kategori_kendaraan SET 
                asset_category_id       ='$ASSET_CATEGORY_ID',
                asset_category_code     ='$ASSET_CATEGORY_CODE',
                asset_category_name     ='$ASSET_CATEGORY_NAME',
                asset_type_id           ='$ASSET_TYPE_ID',
                is_active               ='$IS_ACTIVE',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'
             ON DUPLICATE KEY UPDATE
                asset_category_id       ='$ASSET_CATEGORY_ID',
                asset_category_code     ='$ASSET_CATEGORY_CODE',
                asset_category_name     ='$ASSET_CATEGORY_NAME',
                asset_type_id           ='$ASSET_TYPE_ID',
                is_active               ='$IS_ACTIVE',
                usr_crt                 ='$USR_CRT',
                dtm_crt                 ='$DTM_CRT',
                usr_upd                 ='$USR_UPD',
                dtm_upd                 ='$DTM_UPD'  ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc9 += $no;
    }else{
        $err9 += $no;
         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_ASSET_CATEGORY',
                    sync_error  ='$ASSET_CATEGORY_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_ASSET_CATEGORY',
                sync_success    ='$suc9',
                sync_error      ='$err9',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_ASSET_CATEGORY OK <br>";

// 10
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REF_PROFESSION (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $PROFESSION_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['PROFESSION_CODE']);
        $PROFESSION_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['PROFESSION_NAME']);
   
        $no =1;
   $sqlin = "INSERT INTO cc_master_profession SET 
                profession_code         ='$PROFESSION_CODE',    
                profession_name         ='$PROFESSION_NAME'
             ON DUPLICATE KEY UPDATE
                profession_code         ='$PROFESSION_CODE',    
                profession_name         ='$PROFESSION_NAME'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc10 += $no;
    }else{
        $err10 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_REF_PROFESSION',
                    sync_error  ='$PROFESSION_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_REF_PROFESSION',
            sync_success    ='$suc10',
            sync_error      ='$err10',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_PROFESSION OK <br>";

// 11
//update disable all 
$sqlupsd = "UPDATE cc_master_alamat SET is_active='0' ";
mysqli_query($dbopen,$sqlupsd);

$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REF_ZIPCODE (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $CITY                 = mysqli_real_escape_string($dbopen,$rcs_1['CITY']);
        $KELURAHAN            = mysqli_real_escape_string($dbopen,$rcs_1['KELURAHAN']);
        $KECAMATAN            = mysqli_real_escape_string($dbopen,$rcs_1['KECAMATAN']);
        $ZIPCODE              = mysqli_real_escape_string($dbopen,$rcs_1['ZIPCODE']);
        $SUB_ZIPCODE          = mysqli_real_escape_string($dbopen,$rcs_1['SUB_ZIPCODE']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_alamat SET 
                city            ='$CITY',
                kecamatan       ='$KECAMATAN',
                kelurahan       ='$KELURAHAN',
                zipcode         ='$ZIPCODE',
                sub_zipcode     ='$SUB_ZIPCODE',
                is_active       ='1'
             ON DUPLICATE KEY UPDATE
                city            ='$CITY',
                kecamatan       ='$KECAMATAN',
                kelurahan       ='$KELURAHAN',
                zipcode         ='$ZIPCODE',
                sub_zipcode     ='$SUB_ZIPCODE',
                is_active       ='1' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc11 += $no;
    }else{
        $err11 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_REF_ZIPCODE',
                    sync_error  ='$CITY',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_REF_ZIPCODE',
            sync_success    ='$suc11',
            sync_error      ='$err11',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_ZIPCODE OK <br>";

// 12
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REF_MASTER_RELIGION (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $MASTER_CODE      = mysqli_real_escape_string($dbopen,$rcs_1['MASTER_CODE']);
        $DESCR            = mysqli_real_escape_string($dbopen,$rcs_1['DESCR']);
   
        $no =1;
   $sqlin = "INSERT INTO cc_master_religion SET 
                religion_code       ='$MASTER_CODE',
                religion_name       ='$DESCR' 
             ON DUPLICATE KEY UPDATE
                religion_code       ='$MASTER_CODE',
                religion_name       ='$DESCR'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        
        $suc12 += $no;
    }else{
        $err12 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                    sync_desc   ='STG_M_CC_CRM_REF_MASTER_RELIGION',
                    sync_error  ='$MASTER_CODE',
                    sync_time   =now()";
            mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_REF_MASTER_RELIGION',
            sync_success    ='$suc12',
            sync_error      ='$err12',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_MASTER_RELIGION OK <br>";

// 13
$mss_1 = "SELECT * FROM WISE_STAGING..STG_M_CC_CRM_REF_EMP (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $REF_EMP_ID         = mysqli_real_escape_string($dbopen,$rcs_1['REF_EMP_ID']);
        $EMP_NO             = mysqli_real_escape_string($dbopen,$rcs_1['EMP_NO']);
        $EMP_NAME           = mysqli_real_escape_string($dbopen,$rcs_1['EMP_NAME']);
        $JOIN_DT            = mysqli_real_escape_string($dbopen,$rcs_1['JOIN_DT']);
        $JOIN_DT            = date("Y-m-d h:i:s", strtotime($JOIN_DT));
        $USR_UPD            = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD            = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
        $DTM_UPD            = date("Y-m-d h:i:s", strtotime($DTM_UPD));
        $USR_CRT            = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT            = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $DTM_CRT            = date("Y-m-d h:i:s", strtotime($DTM_CRT));
        $IS_ACTIVE          = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $BRANCH_CODE        = mysqli_real_escape_string($dbopen,$rcs_1['BRANCH_CODE']);
        $BRANCH_NAME        = mysqli_real_escape_string($dbopen,$rcs_1['BRANCH_NAME']);
   
   $no =1;
   /*
   $passwd = md5("123456");
   $sqlin = "INSERT INTO cc_agent_profile SET 
                ref_emp_id      ='$REF_EMP_ID',
                emp_no          ='$EMP_NO',
                agent_id        ='$EMP_NO',
                agent_name      ='$EMP_NAME',
                join_date       ='$JOIN_DT',
                usr_upd         ='$USR_UPD',
                dtm_upd         ='$DTM_UPD',
                usr_crt         ='$USR_CRT',
                dtm_crt         ='$DTM_CRT',
                is_active       ='$IS_ACTIVE',
                `status`        ='$IS_ACTIVE',
                agent_password  ='$passwd',
                agent_level     ='1',
                created_by      ='99999',
                insert_time     =now() 
            ON DUPLICATE KEY UPDATE
                ref_emp_id      ='$REF_EMP_ID',
                emp_no          ='$EMP_NO',
                agent_id        ='$EMP_NO',
                agent_name      ='$EMP_NAME',
                join_date       ='$JOIN_DT',
                usr_upd         ='$USR_UPD',
                dtm_upd         ='$DTM_UPD',
                usr_crt         ='$USR_CRT',
                dtm_crt         ='$DTM_CRT',
                is_active       ='$IS_ACTIVE',
                `status`        ='$IS_ACTIVE',
                agent_password  ='$passwd',
                agent_level     ='1',
                created_by      ='99999',
                insert_time     =now() "; */ 
    $sqlin = "INSERT INTO cc_employee SET
                    ref_emp_id      ='$REF_EMP_ID',
                    ref_no          ='$EMP_NO',
                    emp_name        ='$EMP_NAME',
                    join_dt         ='$JOIN_DT',
                    usr_upd         ='$USR_UPD',
                    dtm_upd         ='$DTM_UPD',        
                    usr_crt         ='$USR_CRT',
                    dtm_crt         ='$DTM_CRT',
                    id_active       ='$IS_ACTIVE',
                    branch_code     ='$BRANCH_CODE',
                    branch_name     ='$BRANCH_NAME',
                    create_by       ='99999',
                    create_time     =now()
                ON DUPLICATE KEY UPDATE 
                    ref_emp_id      ='$REF_EMP_ID',
                    ref_no          ='$EMP_NO',
                    emp_name        ='$EMP_NAME',
                    join_dt         ='$JOIN_DT',
                    usr_upd         ='$USR_UPD',
                    dtm_upd         ='$DTM_UPD',        
                    usr_crt         ='$USR_CRT',
                    dtm_crt         ='$DTM_CRT',
                    id_active       ='$IS_ACTIVE',
                    branch_code     ='$BRANCH_CODE',
                    branch_name     ='$BRANCH_NAME',
                    update_by       ='99999',
                    update_time     =now() ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc13 += $no;
    }else{
        $err13 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_REF_EMP',
                        sync_error  ='$REF_EMP_ID',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }
}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_REF_EMP',
            sync_success    ='$suc13',
            sync_error      ='$err13',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REF_EMP OK <br>";

// 14
$sqlflag = "UPDATE cc_prod_offering SET `status`=0";
$resflag = mysqli_query($dbopen,$sqlflag);

$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_PROD_OFFERING (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $PROD_OFFERING_CODE         = mysqli_real_escape_string($dbopen,$rcs_1['PROD_OFFERING_CODE']);
        $PROD_OFFERING_NAME         = mysqli_real_escape_string($dbopen,$rcs_1['PROD_OFFERING_NAME']);
   
   $no =1;
   
   $sqlin = "INSERT INTO cc_prod_offering SET 
                prod_offering_code     ='$PROD_OFFERING_CODE',    
                prod_offering_name     ='$PROD_OFFERING_NAME',
                status                 ='1'
             ON DUPLICATE KEY UPDATE
                prod_offering_code     ='$PROD_OFFERING_CODE',    
                prod_offering_name     ='$PROD_OFFERING_NAME',
                status                 ='1' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc14 += $no;
    }else{
        $err14 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_PROD_OFFERING',
                        sync_error  ='$PROD_OFFERING_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_PROD_OFFERING',
            sync_success    ='$suc14',
            sync_error      ='$err14',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_PROD_OFFERING OK <br>";

// 15
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_REFERANTOR_X (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $REFERANTOR_X_ID            = mysqli_real_escape_string($dbopen,$rcs_1['REFERANTOR_X_ID']);
        $REFERANTOR_NO              = mysqli_real_escape_string($dbopen,$rcs_1['REFERANTOR_NO']);
        $MR_REFERANTOR_GROUP        = mysqli_real_escape_string($dbopen,$rcs_1['MR_REFERANTOR_GROUP']);
        $REFERANTOR_TYPE            = mysqli_real_escape_string($dbopen,$rcs_1['REFERANTOR_TYPE']);
        $REFERANTOR_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['REFERANTOR_NAME']);
        $CUST_ID                    = mysqli_real_escape_string($dbopen,$rcs_1['CUST_ID']);
        $IS_ACTIVE                  = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $IS_MOTORKU                 = mysqli_real_escape_string($dbopen,$rcs_1['IS_MOTORKU']);
        $MR_JOB_POSITION            = mysqli_real_escape_string($dbopen,$rcs_1['MR_JOB_POSITION']);
        $IS_MOBILKU                 = mysqli_real_escape_string($dbopen,$rcs_1['IS_MOBILKU']);
        $REF_EMP_ID                 = mysqli_real_escape_string($dbopen,$rcs_1['REF_EMP_ID']);
        $USR_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
        $USR_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $FLAG                       = mysqli_real_escape_string($dbopen,$rcs_1['FLAG']);

        $REF_EMP_ID  = trim($REF_EMP_ID);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_referantor SET 
                referantor_id       ='$REFERANTOR_X_ID',
                referantor_no       ='$REFERANTOR_NO',
                referantor_group    ='$MR_REFERANTOR_GROUP',
                referantor_type     ='$REFERANTOR_TYPE',
                referantor_name     ='$REFERANTOR_NAME',
                cust_id             ='$CUST_ID',
                is_active           ='$IS_ACTIVE',
                ref_emp_id          ='$REF_EMP_ID',
                is_motorku          ='$IS_MOTORKU',
                mr_job_position     ='$MR_JOB_POSITION',
                is_mobilku          ='$IS_MOBILKU',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                flag                ='$FLAG'
             ON DUPLICATE KEY UPDATE
                referantor_id       ='$REFERANTOR_X_ID',
                referantor_no       ='$REFERANTOR_NO',
                referantor_group    ='$MR_REFERANTOR_GROUP',
                referantor_type     ='$REFERANTOR_TYPE',
                referantor_name     ='$REFERANTOR_NAME',
                cust_id             ='$CUST_ID',
                is_active           ='$IS_ACTIVE',
                ref_emp_id          ='$REF_EMP_ID',
                is_motorku          ='$IS_MOTORKU',
                mr_job_position     ='$MR_JOB_POSITION',
                is_mobilku          ='$IS_MOBILKU',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                flag                ='$FLAG' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc15 += $no;
    }else{
        $err15 += $no;
        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_REFERANTOR_X',
                        sync_error  ='$REFERANTOR_X_ID',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_REFERANTOR_X',
                sync_success    ='$suc15',
                sync_error      ='$err15',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_REFERANTOR_X OK <br>";

// 16
$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_OFFICE_REGION_X (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $OFFICE_REGION_X_ID         = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_REGION_X_ID']);
        $OFFICE_REGION_CODE         = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_REGION_CODE']);
        $OFFICE_REGION_NAME         = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_REGION_NAME']);
        $IS_ACTIVE                  = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $DTM_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $USR_CRT                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $USR_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
   
   $no =1;
   $sqlin = "INSERT INTO cc_master_region SET 
                region_id           ='$OFFICE_REGION_X_ID',
                region_code         ='$OFFICE_REGION_CODE',
                region_name         ='$OFFICE_REGION_NAME',
                is_active           ='$IS_ACTIVE',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD'
            ON DUPLICATE KEY UPDATE
                region_id           ='$OFFICE_REGION_X_ID',
                region_code         ='$OFFICE_REGION_CODE',
                region_name         ='$OFFICE_REGION_NAME',
                is_active           ='$IS_ACTIVE',
                usr_crt             ='$USR_CRT',
                dtm_crt             ='$DTM_CRT',
                usr_upd             ='$USR_UPD',
                dtm_upd             ='$DTM_UPD'";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc16 += $no;
    }else{
        $err16 += $no;

         //error 
         $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_OFFICE_REGION_X',
                        sync_error  ='$OFFICE_REGION_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
                sync_desc       ='STG_M_CC_CRM_OFFICE_REGION_X',
                sync_success    ='$suc16',
                sync_error      ='$err16',
                sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_OFFICE_REGION_X OK <br>";


//17
$mss_1 = "select * from WISE_STAGING..M_MKT_POLO_MAPPING_PRODUCT_MTRKU (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $m_mkt_polo_mapping_product_mtrku_id = mysqli_real_escape_string($dbopen,$rcs_1['m_mkt_polo_mapping_product_mtrku_id']);
        $offering_code_30                    = mysqli_real_escape_string($dbopen,$rcs_1['offering_code_30']);
        $offering_name_30                    = mysqli_real_escape_string($dbopen,$rcs_1['offering_name_30']);
        $offering_code_120                   = mysqli_real_escape_string($dbopen,$rcs_1['offering_code_120']);
        $offering_name_120                   = mysqli_real_escape_string($dbopen,$rcs_1['offering_name_120']);
        $is_active                           = mysqli_real_escape_string($dbopen,$rcs_1['is_active']);        
        $dtm_crt                             = mysqli_real_escape_string($dbopen,$rcs_1['dtm_crt']);
        $usr_crt                             = mysqli_real_escape_string($dbopen,$rcs_1['usr_crt']);       
        $dtm_upd                             = mysqli_real_escape_string($dbopen,$rcs_1['dtm_upd']);
        $usr_upd                             = mysqli_real_escape_string($dbopen,$rcs_1['usr_upd']);

   $no =1;
   $sqlin = "INSERT INTO cc_master_mapping_product_mtr SET 
                mapping_product_id      ='$m_mkt_polo_mapping_product_mtrku_id',
                offering_code_30        ='$offering_code_30',
                offering_name_30        ='$offering_name_30',
                offering_code_120       ='$offering_code_120',
                offering_name_120       ='$offering_name_120',
                is_active               ='$is_active',
                dtm_crt                 ='$dtm_crt',
                usr_crt                 ='$usr_crt',
                dtm_upd                 ='$dtm_upd',
                usr_upd                 ='$usr_upd'
             ON DUPLICATE KEY UPDATE
                mapping_product_id      ='$m_mkt_polo_mapping_product_mtrku_id',
                offering_code_30        ='$offering_code_30',
                offering_name_30        ='$offering_name_30',
                offering_code_120       ='$offering_code_120',
                offering_name_120       ='$offering_name_120',
                is_active               ='$is_active',
                dtm_crt                 ='$dtm_crt',
                usr_crt                 ='$usr_crt',
                dtm_upd                 ='$dtm_upd',
                usr_upd                 ='$usr_upd' ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc17 += $no;
    }else{
        $err17 += $no;
           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='M_MKT_POLO_MAPPING_PRODUCT_MTRKU',
                        sync_error  ='$m_mkt_polo_mapping_product_mtrku_id',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='M_MKT_POLO_MAPPING_PRODUCT_MTRKU',
            sync_success    ='$suc17',
            sync_error      ='$err17',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data M_MKT_POLO_MAPPING_PRODUCT_MTRKU OK <br>";



//new phase 1 = update 

//18 
$sqlflag = "UPDATE cc_master_supplier SET sync_status=0";
$resflag = mysqli_query($dbopen,$sqlflag);

$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_SUPPL (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $SUPPL_ID                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_ID']);
        $SUPPL_CODE                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_CODE']);
        $SUPPL_NAME                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_NAME']);
        $SUPPL_SHORT_NAME               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_SHORT_NAME']);
        $SUPPL_LEGAL_NAME               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_LEGAL_NAME']);
        $SUPPL_HOLDING_ID               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_ID']);
        $SUPPL_TYPE                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_TYPE']);
        $REF_SUPPL_CLASS_ID             = mysqli_real_escape_string($dbopen,$rcs_1['REF_SUPPL_CLASS_ID']);
        $IS_ONE_AFFILIATE               = mysqli_real_escape_string($dbopen,$rcs_1['IS_ONE_AFFILIATE']);
        $MR_INCENTIVE_CALC_METHOD       = mysqli_real_escape_string($dbopen,$rcs_1['MR_INCENTIVE_CALC_METHOD']);
        $NPWP                           = mysqli_real_escape_string($dbopen,$rcs_1['NPWP']);
        $TDP                            = mysqli_real_escape_string($dbopen,$rcs_1['TDP']);
        $SIUP                           = mysqli_real_escape_string($dbopen,$rcs_1['SIUP']);
        $SUPPL_ADDR                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_ADDR']);
        $SUPPL_RT                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_RT']);
        $SUPPL_RW                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_RW']);
        $SUPPL_KELURAHAN                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_KELURAHAN']);
        $SUPPL_KECAMATAN                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_KECAMATAN']);
        $SUPPL_CITY                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_CITY']);
        $SUPPL_ZIPCODE                  = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_ZIPCODE']);
        $SUPPL_AREA_PHN_1               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_AREA_PHN_1']);
        $SUPPL_PHN_1                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_1']);
        $SUPPL_AREA_PHN_2               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_AREA_PHN_2']);
        $SUPPL_PHN_2                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_2']);
        $SUPPL_AREA_FAX                 = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_AREA_FAX']);
        $SUPPL_FAX                      = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_FAX']);
        $IS_ACTIVE                      = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);
        $SUPPL_BAD_STATUS               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BAD_STATUS']);
        $START_DT                       = mysqli_real_escape_string($dbopen,$rcs_1['START_DT']);
        $USR_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);
        $DTM_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);
        $USR_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);
        $DTM_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);
        $SUPPL_AREA_PHN_3               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_AREA_PHN_3']);
        $SUPPL_PHN_3                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_3']);
        $SUPPL_PHN_EXT_1                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_EXT_1']);
        $SUPPL_PHN_EXT_2                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_EXT_2']);
        $SUPPL_PHN_EXT_3                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_PHN_EXT_3']);
        $INCENTIVE_CARD_ID              = mysqli_real_escape_string($dbopen,$rcs_1['INCENTIVE_CARD_ID']);
        $PIB_SCHM_ID                    = mysqli_real_escape_string($dbopen,$rcs_1['PIB_SCHM_ID']);
        $DIC_SCHM_ID                    = mysqli_real_escape_string($dbopen,$rcs_1['DIC_SCHM_ID']);
        $NPWP_NAME                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_NAME']);
        $NPWP_ADDR                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_ADDR']);
        $NPWP_RT                        = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_RT']);
        $NPWP_RW                        = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_RW']);
        $NPWP_ZIPCODE                   = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_ZIPCODE']);
        $NPWP_KELURAHAN                 = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_KELURAHAN']);
        $NPWP_KECAMATAN                 = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_KECAMATAN']);
        $NPWP_CITY                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_CITY']);
        $SUPPL_NO                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_NO']);
        $MR_SUPPL_UP_CALC_MTHD          = mysqli_real_escape_string($dbopen,$rcs_1['MR_SUPPL_UP_CALC_MTHD']);
        $IDENTITY_NO                    = mysqli_real_escape_string($dbopen,$rcs_1['IDENTITY_NO']);
        $MR_ID_TYPE                     = mysqli_real_escape_string($dbopen,$rcs_1['MR_ID_TYPE']);

   $no =1;
   $sqlin = "INSERT INTO cc_master_supplier SET 
                suppl_id                        = '$SUPPL_ID',
                suppl_code                      = '$SUPPL_CODE',
                suppl_name                      = '$SUPPL_NAME',
                suppl_short_name                = '$SUPPL_SHORT_NAME',
                suppl_legal_name                = '$SUPPL_LEGAL_NAME',
                suppl_holding_id                = '$SUPPL_HOLDING_ID',
                suppl_type                      = '$SUPPL_TYPE',
                ref_suppl_class_id              = '$REF_SUPPL_CLASS_ID',
                is_one_affiliate                = '$IS_ONE_AFFILIATE',
                mr_incentive_calc_method        = '$MR_INCENTIVE_CALC_METHOD',
                npwp                            = '$NPWP',
                tdp                             = '$TDP',
                siup                            = '$SIUP',
                suppl_addr                      = '$SUPPL_ADDR',
                suppl_rt                        = '$SUPPL_RT',
                suppl_rw                        = '$SUPPL_RW',
                suppl_kelurahan                 = '$SUPPL_KELURAHAN',
                suppl_kecamatan                 = '$SUPPL_KECAMATAN',
                suppl_city                      = '$SUPPL_CITY',
                suppl_zipcode                   = '$SUPPL_ZIPCODE',
                suppl_area_phn_1                = '$SUPPL_AREA_PHN_1',
                suppl_phn_1                     = '$SUPPL_PHN_1',
                suppl_area_phn_2                = '$SUPPL_AREA_PHN_2',
                suppl_phn_2                     = '$SUPPL_PHN_2',
                suppl_area_fax                  = '$SUPPL_AREA_FAX',
                suppl_fax                       = '$SUPPL_FAX',
                is_active                       = '$IS_ACTIVE',
                suppl_bad_status                = '$SUPPL_BAD_STATUS',
                start_dt                        = '$START_DT',
                usr_upd                         = '$USR_UPD',
                dtm_upd                         = '$DTM_UPD',
                usr_crt                         = '$USR_CRT',
                dtm_crt                         = '$DTM_CRT',
                suppl_area_phn_3                = '$SUPPL_AREA_PHN_3',
                suppl_phn_3                     = '$SUPPL_PHN_3',
                suppl_phn_ext_1                 = '$SUPPL_PHN_EXT_1',
                suppl_phn_ext_2                 = '$SUPPL_PHN_EXT_2',
                suppl_phn_ext_3                 = '$SUPPL_PHN_EXT_3',
                incentive_card_id               = '$INCENTIVE_CARD_ID',
                pib_schm_id                     = '$PIB_SCHM_ID',
                dic_schm_id                     = '$DIC_SCHM_ID',
                npwp_name                       = '$NPWP_NAME',
                npwp_addr                       = '$NPWP_ADDR',
                npwp_rt                         = '$NPWP_RT',
                npwp_rw                         = '$NPWP_RW',
                npwp_zipcode                    = '$NPWP_ZIPCODE',
                npwp_kelurahan                  = '$NPWP_KELURAHAN',
                npwp_kecamatan                  = '$NPWP_KECAMATAN',
                npwp_city                       = '$NPWP_CITY',
                suppl_no                        = '$SUPPL_NO',
                mr_suppl_up_calc_mthd           = '$MR_SUPPL_UP_CALC_MTHD',
                identity_no                     = '$IDENTITY_NO',
                mr_id_type                      = '$MR_ID_TYPE',
                sync_status                     = '1',
                sync_time                       = now()
             ON DUPLICATE KEY UPDATE
                suppl_id                        = '$SUPPL_ID',
                suppl_code                      = '$SUPPL_CODE',
                suppl_name                      = '$SUPPL_NAME',
                suppl_short_name                = '$SUPPL_SHORT_NAME',
                suppl_legal_name                = '$SUPPL_LEGAL_NAME',
                suppl_holding_id                = '$SUPPL_HOLDING_ID',
                suppl_type                      = '$SUPPL_TYPE',
                ref_suppl_class_id              = '$REF_SUPPL_CLASS_ID',
                is_one_affiliate                = '$IS_ONE_AFFILIATE',
                mr_incentive_calc_method        = '$MR_INCENTIVE_CALC_METHOD',
                npwp                            = '$NPWP',
                tdp                             = '$TDP',
                siup                            = '$SIUP',
                suppl_addr                      = '$SUPPL_ADDR',
                suppl_rt                        = '$SUPPL_RT',
                suppl_rw                        = '$SUPPL_RW',
                suppl_kelurahan                 = '$SUPPL_KELURAHAN',
                suppl_kecamatan                 = '$SUPPL_KECAMATAN',
                suppl_city                      = '$SUPPL_CITY',
                suppl_zipcode                   = '$SUPPL_ZIPCODE',
                suppl_area_phn_1                = '$SUPPL_AREA_PHN_1',
                suppl_phn_1                     = '$SUPPL_PHN_1',
                suppl_area_phn_2                = '$SUPPL_AREA_PHN_2',
                suppl_phn_2                     = '$SUPPL_PHN_2',
                suppl_area_fax                  = '$SUPPL_AREA_FAX',
                suppl_fax                       = '$SUPPL_FAX',
                is_active                       = '$IS_ACTIVE',
                suppl_bad_status                = '$SUPPL_BAD_STATUS',
                start_dt                        = '$START_DT',
                usr_upd                         = '$USR_UPD',
                dtm_upd                         = '$DTM_UPD',
                usr_crt                         = '$USR_CRT',
                dtm_crt                         = '$DTM_CRT',
                suppl_area_phn_3                = '$SUPPL_AREA_PHN_3',
                suppl_phn_3                     = '$SUPPL_PHN_3',
                suppl_phn_ext_1                 = '$SUPPL_PHN_EXT_1',
                suppl_phn_ext_2                 = '$SUPPL_PHN_EXT_2',
                suppl_phn_ext_3                 = '$SUPPL_PHN_EXT_3',
                incentive_card_id               = '$INCENTIVE_CARD_ID',
                pib_schm_id                     = '$PIB_SCHM_ID',
                dic_schm_id                     = '$DIC_SCHM_ID',
                npwp_name                       = '$NPWP_NAME',
                npwp_addr                       = '$NPWP_ADDR',
                npwp_rt                         = '$NPWP_RT',
                npwp_rw                         = '$NPWP_RW',
                npwp_zipcode                    = '$NPWP_ZIPCODE',
                npwp_kelurahan                  = '$NPWP_KELURAHAN',
                npwp_kecamatan                  = '$NPWP_KECAMATAN',
                npwp_city                       = '$NPWP_CITY',
                suppl_no                        = '$SUPPL_NO',
                mr_suppl_up_calc_mthd           = '$MR_SUPPL_UP_CALC_MTHD',
                identity_no                     = '$IDENTITY_NO',
                mr_id_type                      = '$MR_ID_TYPE',
                sync_status                     = '1',
                sync_time                       = now() ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc18 += $no;
    }else{
        $err18 += $no;
           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_SUPPL',
                        sync_error  ='$SUPPL_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_SUPPL',
            sync_success    ='$suc18',
            sync_error      ='$err18',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_SUPPL OK <br>";

//19 
$sqlflag = "UPDATE cc_master_supplier_branch SET sync_status=0";
$resflag = mysqli_query($dbopen,$sqlflag);

$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_SUPPL_BRANCH (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
    $SUPPL_BRANCH_ID                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_ID']);  
    $SUPPL_ID                       = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_ID']);  
    $SUPPL_BRANCH_CODE              = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_CODE']);  
    $NPWP                           = mysqli_real_escape_string($dbopen,$rcs_1['NPWP']);  
    $SUPPL_BRANCH_AREA              = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_AREA']);  
    $SUPPL_BRANCH_NAME              = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_NAME']);  
    $SUPPL_BRANCH_ADDR              = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_ADDR']);  
    $SUPPL_BRANCH_RT                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_RT']);  
    $SUPPL_BRANCH_RW                = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_RW']);  
    $SUPPL_BRANCH_KELURAHAN         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_KELURAHAN']);  
    $SUPPL_BRANCH_KECAMATAN         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_KECAMATAN']);  
    $SUPPL_BRANCH_CITY              = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_CITY']);  
    $SUPPL_BRANCH_ZIPCODE           = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_ZIPCODE']);  
    $SUPPL_BRANCH_AREA_PHN_1        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_AREA_PHN_1']);  
    $SUPPL_BRANCH_PHN_1             = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_1']);  
    $SUPPL_BRANCH_AREA_PHN_2        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_AREA_PHN_2']);  
    $SUPPL_BRANCH_PHN_2             = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_2']);  
    $SUPPL_BRANCH_AREA_FAX          = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_AREA_FAX']);  
    $SUPPL_BRANCH_FAX               = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_FAX']);  
    $IS_ACTIVE                      = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']);  
    $USR_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']);  
    $DTM_UPD                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']);  
    $USR_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']);  
    $DTM_CRT                        = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']);  
    $SUPPL_BAD_STAT                 = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BAD_STAT']);  
    $SUPPL_BRANCH_AREA_PHN_3        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_AREA_PHN_3']);  
    $SUPPL_BRANCH_PHN_3             = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_3']);  
    $SUPPL_BRANCH_PHN_EXT_1         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_EXT_1']);  
    $SUPPL_BRANCH_PHN_EXT_2         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_EXT_2']);
    $SUPPL_BRANCH_PHN_EXT_3         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_BRANCH_PHN_EXT_3']);
    $SUPPL_AREA_ID                  = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_AREA_ID']);
    $NPWP_NAME                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_NAME']);
    $MAIN_DOC_AGING                 = mysqli_real_escape_string($dbopen,$rcs_1['MAIN_DOC_AGING']);
    $AP_DUE_AFTER_GLV               = mysqli_real_escape_string($dbopen,$rcs_1['AP_DUE_AFTER_GLV']);
    $MR_ASSET_CONDITION             = mysqli_real_escape_string($dbopen,$rcs_1['MR_ASSET_CONDITION']);
    $MARKETING_ID                   = mysqli_real_escape_string($dbopen,$rcs_1['MARKETING_ID']);
    $NPWP_ADDR                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_ADDR']);
    $NPWP_RT                        = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_RT']);
    $NPWP_RW                        = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_RW']);
    $NPWP_ZIPCODE                   = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_ZIPCODE']);
    $NPWP_KELURAHAN                 = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_KELURAHAN']);
    $NPWP_KECAMATAN                 = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_KECAMATAN']);
    $NPWP_CITY                      = mysqli_real_escape_string($dbopen,$rcs_1['NPWP_CITY']);
    $IS_PKP                         = mysqli_real_escape_string($dbopen,$rcs_1['IS_PKP']);
    $IDENTITY_NO                    = mysqli_real_escape_string($dbopen,$rcs_1['IDENTITY_NO']);
    $MR_ID_TYPE                     = mysqli_real_escape_string($dbopen,$rcs_1['MR_ID_TYPE']);
        


   $no =1;
   $sqlin = "INSERT INTO cc_master_supplier_branch SET 
                suppl_branch_id         = '$SUPPL_BRANCH_ID',
                suppl_id                = '$SUPPL_ID',
                suppl_branch_code       = '$SUPPL_BRANCH_CODE',    
                npwp                    = '$NPWP',
                suppl_branch_area       = '$SUPPL_BRANCH_AREA',
                suppl_branch_name       = '$SUPPL_BRANCH_NAME',
                suppl_branch_addr       = '$SUPPL_BRANCH_ADDR',
                suppl_branch_rt         = '$SUPPL_BRANCH_RT',
                suppl_branch_rw         = '$SUPPL_BRANCH_RW',
                suppl_branch_kelurahan  = '$SUPPL_BRANCH_KELURAHAN',
                suppl_branch_kecamatan  = '$SUPPL_BRANCH_KECAMATAN',
                suppl_branch_city       = '$SUPPL_BRANCH_CITY',    
                suppl_branch_zipcode    = '$SUPPL_BRANCH_ZIPCODE',
                suppl_branch_area_phn_1 = '$SUPPL_BRANCH_AREA_PHN_1',
                suppl_branch_phn_1      = '$SUPPL_BRANCH_PHN_1',
                suppl_branch_area_phn_2 = '$SUPPL_BRANCH_AREA_PHN_2',
                suppl_branch_phn_2      = '$SUPPL_BRANCH_PHN_2',
                suppl_branch_area_fax   = '$SUPPL_BRANCH_AREA_FAX',
                suppl_branch_fax        = '$SUPPL_BRANCH_FAX',
                is_active               = '$IS_ACTIVE',
                usr_upd                 = '$USR_UPD',
                dtm_upd                 = '$DTM_UPD',
                usr_crt                 = '$USR_CRT',
                dtm_crt                 = '$DTM_CRT',
                suppl_bad_stat          = '$SUPPL_BAD_STAT',
                suppl_branch_area_phn_3 = '$SUPPL_BRANCH_AREA_PHN_3',
                suppl_branch_phn_3      = '$SUPPL_BRANCH_PHN_3',
                suppl_branch_phn_ext_1  = '$SUPPL_BRANCH_PHN_EXT_1',
                suppl_branch_phn_ext_2  = '$SUPPL_BRANCH_PHN_EXT_2',
                suppl_branch_phn_ext_3  = '$SUPPL_BRANCH_PHN_EXT_3',
                suppl_area_id           = '$SUPPL_AREA_ID',
                npwp_name               = '$NPWP_NAME',
                main_doc_aging          = '$MAIN_DOC_AGING',
                ap_due_after_glv        = '$AP_DUE_AFTER_GLV',
                mr_asset_condition      = '$MR_ASSET_CONDITION',
                marketing_id            = '$MARKETING_ID',     
                npwp_addr               = '$NPWP_ADDR',
                npwp_rt                 = '$NPWP_RT',
                npwp_rw                 = '$NPWP_RW',
                npwp_zipcode            = '$NPWP_ZIPCODE',
                npwp_kelurahan          = '$NPWP_KELURAHAN',
                npwp_kecamatan          = '$NPWP_KECAMATAN',
                npwp_city               = '$NPWP_CITY',
                is_pkp                  = '$IS_PKP',
                identity_no             = '$IDENTITY_NO',
                mr_id_type              = '$MR_ID_TYPE',
                sync_status             = '1',
                sync_time               = now()
             ON DUPLICATE KEY UPDATE
                suppl_branch_id         = '$SUPPL_BRANCH_ID',
                suppl_id                = '$SUPPL_ID',
                suppl_branch_code       = '$SUPPL_BRANCH_CODE',    
                npwp                    = '$NPWP',
                suppl_branch_area       = '$SUPPL_BRANCH_AREA',
                suppl_branch_name       = '$SUPPL_BRANCH_NAME',
                suppl_branch_addr       = '$SUPPL_BRANCH_ADDR',
                suppl_branch_rt         = '$SUPPL_BRANCH_RT',
                suppl_branch_rw         = '$SUPPL_BRANCH_RW',
                suppl_branch_kelurahan  = '$SUPPL_BRANCH_KELURAHAN',
                suppl_branch_kecamatan  = '$SUPPL_BRANCH_KECAMATAN',
                suppl_branch_city       = '$SUPPL_BRANCH_CITY',    
                suppl_branch_zipcode    = '$SUPPL_BRANCH_ZIPCODE',
                suppl_branch_area_phn_1 = '$SUPPL_BRANCH_AREA_PHN_1',
                suppl_branch_phn_1      = '$SUPPL_BRANCH_PHN_1',
                suppl_branch_area_phn_2 = '$SUPPL_BRANCH_AREA_PHN_2',
                suppl_branch_phn_2      = '$SUPPL_BRANCH_PHN_2',
                suppl_branch_area_fax   = '$SUPPL_BRANCH_AREA_FAX',
                suppl_branch_fax        = '$SUPPL_BRANCH_FAX',
                is_active               = '$IS_ACTIVE',
                usr_upd                 = '$USR_UPD',
                dtm_upd                 = '$DTM_UPD',
                usr_crt                 = '$USR_CRT',
                dtm_crt                 = '$DTM_CRT',
                suppl_bad_stat          = '$SUPPL_BAD_STAT',
                suppl_branch_area_phn_3 = '$SUPPL_BRANCH_AREA_PHN_3',
                suppl_branch_phn_3      = '$SUPPL_BRANCH_PHN_3',
                suppl_branch_phn_ext_1  = '$SUPPL_BRANCH_PHN_EXT_1',
                suppl_branch_phn_ext_2  = '$SUPPL_BRANCH_PHN_EXT_2',
                suppl_branch_phn_ext_3  = '$SUPPL_BRANCH_PHN_EXT_3',
                suppl_area_id           = '$SUPPL_AREA_ID',
                npwp_name               = '$NPWP_NAME',
                main_doc_aging          = '$MAIN_DOC_AGING',
                ap_due_after_glv        = '$AP_DUE_AFTER_GLV',
                mr_asset_condition      = '$MR_ASSET_CONDITION',
                marketing_id            = '$MARKETING_ID',     
                npwp_addr               = '$NPWP_ADDR',
                npwp_rt                 = '$NPWP_RT',
                npwp_rw                 = '$NPWP_RW',
                npwp_zipcode            = '$NPWP_ZIPCODE',
                npwp_kelurahan          = '$NPWP_KELURAHAN',
                npwp_kecamatan          = '$NPWP_KECAMATAN',
                npwp_city               = '$NPWP_CITY',
                is_pkp                  = '$IS_PKP',
                identity_no             = '$IDENTITY_NO',
                mr_id_type              = '$MR_ID_TYPE',
                sync_status             = '1',
                sync_time               = now()";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc19 += $no;
    }else{
        $err19 += $no;
           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_SUPPL_BRANCH',
                        sync_error  ='$SUPPL_ID',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_SUPPL_BRANCH',
            sync_success    ='$suc19',
            sync_error      ='$err19',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_SUPPL_BRANCH OK <br>";

//20
$sqlflag = "UPDATE cc_master_supplier_holding SET sync_status=0";
$resflag = mysqli_query($dbopen,$sqlflag);

$mss_1 = "select * from WISE_STAGING..STG_M_CC_CRM_SUPPL_HOLDING (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
    $SUPPL_HOLDING_ID                           = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_ID']);  
    $SUPPL_HOLDING_NAME                         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_NAME']);  
    $SUPPL_HOLDING_SHORT_NAME                   = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_SHORT_NAME']); 
    $NPWP                                       = mysqli_real_escape_string($dbopen,$rcs_1['NPWP']); 
    $TDP                                        = mysqli_real_escape_string($dbopen,$rcs_1['TDP']); 
    $SIUP                                       = mysqli_real_escape_string($dbopen,$rcs_1['SIUP']); 
    $SUPPL_HOLDING_ADDR                         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_ADDR']); 
    $SUPPL_HOLDING_RT                           = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_RT']); 
    $SUPPL_HOLDING_RW                           = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_RW']); 
    $SUPPL_HOLDING_KELURAHAN                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_KELURAHAN']); 
    $SUPPL_HOLDING_KECAMATAN                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_KECAMATAN']); 
    $SUPPL_HOLDING_CITY                         = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_CITY']); 
    $SUPPL_HOLDING_ZIPCODE                      = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_ZIPCODE']); 
    $SUPPL_HOLDING_AREA_PHN_1                   = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_AREA_PHN_1']); 
    $SUPPL_HOLDING_PHN_1                        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_1']); 
    $SUPPL_HOLDING_AREA_PHN_2                   = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_AREA_PHN_2']); 
    $SUPPL_HOLDING_PHN_2                        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_2']); 
    $SUPPL_HOLDING_AREA_FAX                     = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_AREA_FAX']); 
    $SUPPL_HOLDING_FAX                          = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_FAX']); 
    $IS_ACTIVE                                  = mysqli_real_escape_string($dbopen,$rcs_1['IS_ACTIVE']); 
    $CNTCT_PERSON_NAME                          = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_NAME']); 
    $CNTCT_PERSON_JOB_TITLE                     = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_JOB_TITLE']); 
    $CNTCT_PERSON_AREA_PHN                      = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_AREA_PHN']); 
    $CNTCT_PERSON_PHN                           = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_PHN']); 
    $CNTCT_PERSON_MOBILE_PHN                    = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_MOBILE_PHN']); 
    $CNTCT_PERSON_EMAIL                         = mysqli_real_escape_string($dbopen,$rcs_1['CNTCT_PERSON_EMAIL']); 
    $USR_UPD                                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_UPD']); 
    $DTM_UPD                                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_UPD']); 
    $USR_CRT                                    = mysqli_real_escape_string($dbopen,$rcs_1['USR_CRT']); 
    $DTM_CRT                                    = mysqli_real_escape_string($dbopen,$rcs_1['DTM_CRT']); 
    $SUPPL_HOLDING_AREA_PHN_3                   = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_AREA_PHN_3']); 
    $SUPPL_HOLDING_PHN_3                        = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_3']); 
    $SUPPL_HOLDING_PHN_EXT_1                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_EXT_1']); 
    $SUPPL_HOLDING_PHN_EXT_2                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_EXT_2']); 
    $SUPPL_HOLDING_PHN_EXT_3                    = mysqli_real_escape_string($dbopen,$rcs_1['SUPPL_HOLDING_PHN_EXT_3']); 
    
        


   $no =1;
   $sqlin = "INSERT INTO cc_master_supplier_holding SET 
                suppl_holding_id            = '$SUPPL_HOLDING_ID',
                suppl_holding_name          = '$SUPPL_HOLDING_NAME',
                suppl_holding_short_name    = '$SUPPL_HOLDING_SHORT_NAME',    
                npwp                        = '$NPWP',
                tdp                         = '$TDP',
                siup                        = '$SIUP',
                suppl_holding_addr          = '$SUPPL_HOLDING_ADDR',
                suppl_holding_rt            = '$SUPPL_HOLDING_RT',
                suppl_holding_rw            = '$SUPPL_HOLDING_RW',
                suppl_holding_kelurahan     = '$SUPPL_HOLDING_KELURAHAN',
                suppl_holding_kecamatan     = '$SUPPL_HOLDING_KECAMATAN',
                suppl_holding_city          = '$SUPPL_HOLDING_CITY',
                suppl_holding_zipcode       = '$SUPPL_HOLDING_ZIPCODE',
                suppl_holding_area_phn_1    = '$SUPPL_HOLDING_AREA_PHN_1',
                suppl_holding_phn_1         = '$SUPPL_HOLDING_PHN_1',
                suppl_holding_area_phn_2    = '$SUPPL_HOLDING_AREA_PHN_2',
                suppl_holding_phn_2         = '$SUPPL_HOLDING_PHN_2',
                suppl_holding_area_fax      = '$SUPPL_HOLDING_AREA_FAX',
                suppl_holding_fax           = '$SUPPL_HOLDING_FAX',
                is_active                   = '$IS_ACTIVE',
                cntct_person_name           = '$CNTCT_PERSON_NAME',
                cntct_person_job_title      = '$CNTCT_PERSON_JOB_TITLE',
                cntct_person_area_phn       = '$CNTCT_PERSON_AREA_PHN',
                cntct_person_phn            = '$CNTCT_PERSON_PHN',
                cntct_person_mobile_phn     = '$CNTCT_PERSON_MOBILE_PHN',
                cntct_person_email          = '$CNTCT_PERSON_EMAIL',
                usr_upd                     = '$USR_UPD',
                dtm_upd                     = '$DTM_UPD',
                usr_crt                     = '$USR_CRT',
                dtm_crt                     = '$DTM_CRT',
                suppl_holding_area_phn_3    = '$SUPPL_HOLDING_AREA_PHN_3',
                suppl_holding_phn_3         = '$SUPPL_HOLDING_PHN_3',
                suppl_holding_phn_ext_1     = '$SUPPL_HOLDING_PHN_EXT_1',
                suppl_holding_phn_ext_2     = '$SUPPL_HOLDING_PHN_EXT_2',
                suppl_holding_phn_ext_3     = '$SUPPL_HOLDING_PHN_EXT_3',
                sync_status                 = '1',
                sync_time                   = now()
             ON DUPLICATE KEY UPDATE
                suppl_holding_id            = '$SUPPL_HOLDING_ID',
                suppl_holding_name          = '$SUPPL_HOLDING_NAME',
                suppl_holding_short_name    = '$SUPPL_HOLDING_SHORT_NAME',    
                npwp                        = '$NPWP',
                tdp                         = '$TDP',
                siup                        = '$SIUP',
                suppl_holding_addr          = '$SUPPL_HOLDING_ADDR',
                suppl_holding_rt            = '$SUPPL_HOLDING_RT',
                suppl_holding_rw            = '$SUPPL_HOLDING_RW',
                suppl_holding_kelurahan     = '$SUPPL_HOLDING_KELURAHAN',
                suppl_holding_kecamatan     = '$SUPPL_HOLDING_KECAMATAN',
                suppl_holding_city          = '$SUPPL_HOLDING_CITY',
                suppl_holding_zipcode       = '$SUPPL_HOLDING_ZIPCODE',
                suppl_holding_area_phn_1    = '$SUPPL_HOLDING_AREA_PHN_1',
                suppl_holding_phn_1         = '$SUPPL_HOLDING_PHN_1',
                suppl_holding_area_phn_2    = '$SUPPL_HOLDING_AREA_PHN_2',
                suppl_holding_phn_2         = '$SUPPL_HOLDING_PHN_2',
                suppl_holding_area_fax      = '$SUPPL_HOLDING_AREA_FAX',
                suppl_holding_fax           = '$SUPPL_HOLDING_FAX',
                is_active                   = '$IS_ACTIVE',
                cntct_person_name           = '$CNTCT_PERSON_NAME',
                cntct_person_job_title      = '$CNTCT_PERSON_JOB_TITLE',
                cntct_person_area_phn       = '$CNTCT_PERSON_AREA_PHN',
                cntct_person_phn            = '$CNTCT_PERSON_PHN',
                cntct_person_mobile_phn     = '$CNTCT_PERSON_MOBILE_PHN',
                cntct_person_email          = '$CNTCT_PERSON_EMAIL',
                usr_upd                     = '$USR_UPD',
                dtm_upd                     = '$DTM_UPD',
                usr_crt                     = '$USR_CRT',
                dtm_crt                     = '$DTM_CRT',
                suppl_holding_area_phn_3    = '$SUPPL_HOLDING_AREA_PHN_3',
                suppl_holding_phn_3         = '$SUPPL_HOLDING_PHN_3',
                suppl_holding_phn_ext_1     = '$SUPPL_HOLDING_PHN_EXT_1',
                suppl_holding_phn_ext_2     = '$SUPPL_HOLDING_PHN_EXT_2',
                suppl_holding_phn_ext_3     = '$SUPPL_HOLDING_PHN_EXT_3',
                sync_status                 = '1',
                sync_time                   = now()";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc20 += $no;
    }else{
        $err20 += $no;
           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='STG_M_CC_CRM_SUPPL_HOLDING',
                        sync_error  ='$SUPPL_HOLDING_NAME',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='STG_M_CC_CRM_SUPPL_HOLDING',
            sync_success    ='$suc20',
            sync_error      ='$err20',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data STG_M_CC_CRM_SUPPL_HOLDING OK <br>";

// 21
$sqlflag = "TRUNCATE cc_master_pipeline";
$resflag = mysqli_query($dbopen,$sqlflag);

$mss_1 = "SELECT * FROM WISE_STAGING..PIPELINE WITH(NOLOCK)
          WHERE IS_ACTIVE = 1";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){
        $PIPELINE_ID                = $rcs_1['PIPELINE_ID'];
        $PIPELINE_CODE              = $rcs_1['PIPELINE_CODE'];
        $PIPELINE_NAME              = $rcs_1['PIPELINE_NAME'];
        $IS_ACTIVE                  = $rcs_1['IS_ACTIVE'];
        $PRIORITY_LEVEL             = $rcs_1['PRIORITY_LEVEL'];
        $IS_CHECK_SLIK              = $rcs_1['IS_CHECK_SLIK'];
        $USR_CRT                    = $rcs_1['USR_CRT'];
        $USR_UPD                    = $rcs_1['USR_UPD'];
        $DTM_CRT                    = $rcs_1['DTM_CRT'];
        $DTM_UPD                    = $rcs_1['DTM_UPD'];
   
   $no =1;
   
   $sqlin = "INSERT INTO cc_master_pipeline SET
                        pipeline_id             ='$PIPELINE_ID',
                        pipeline_code           ='$PIPELINE_CODE',
                        pipeline_name           ='$PIPELINE_NAME',
                        priority_level          ='$PRIORITY_LEVEL',
                        is_check_slik           ='$IS_CHECK_SLIK',
                        usr_crt                 ='$USR_CRT',
                        usr_upd                 ='$USR_UPD',
                        dtm_crt                 ='$DTM_CRT',    
                        dtm_upd                 ='$DTM_UPD',  
                        is_active               ='$IS_ACTIVE',
                        sync_time               =now()";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
        $suc14 += $no;
    }else{
        $err14 += $no;

        //error 
        $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='WISE_STAGING_PIPELINE',
                        sync_error  ='$PIPELINE_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}

//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='WISE_STAGING_PIPELINE',
            sync_success    ='$suc14',
            sync_error      ='$err14',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data WISE_STAGING_PIPELINE OK <br>";


//22
$sqlflag = "UPDATE cc_master _pot SET sync_status 0";
$resflag = mysqli_query($dbopen,$sqlflag);

//$mss_1 = "select * from WISE_STAGING..V_PRODUCT_OFFERING (NOLOCK)";
$mss_1 = "select * from WISE_STAGING..V_POT_EXCLUDE (NOLOCK)";
$rss_1 = mssql_query($mss_1);
while($rcs_1 = mssql_fetch_array($rss_1)){//print_r($rcs_1);die();

        $LOB_CODE               = mysqli_real_escape_string($dbopen,$rcs_1['LOB_CODE']);
        $LOB_NAME               = mysqli_real_escape_string($dbopen,$rcs_1['LOB_NAME']);
        $PROD_OFFERING_CODE     = mysqli_real_escape_string($dbopen,$rcs_1['PROD_OFFERING_CODE']);
        $PROD_OFFERING_NAME     = mysqli_real_escape_string($dbopen,$rcs_1['PROD_OFFERING_NAME']);
        $OFFICE_CODE            = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_CODE']);
        $OFFICE_NAME            = mysqli_real_escape_string($dbopen,$rcs_1['OFFICE_NAME']);

   $no =1;
   $sqlin = "INSERT INTO cc_master_pot SET 
                LOB_CODE               = '$LOB_CODE',
                LOB_NAME               = '$LOB_NAME',
                PROD_OFFERING_CODE     = '$PROD_OFFERING_CODE',
                PROD_OFFERING_NAME     = '$PROD_OFFERING_NAME',
                OFFICE_CODE            = '$OFFICE_CODE',
                OFFICE_NAME            = '$OFFICE_NAME',
                sync_status            = '1',
                sync_time              = now()
             ON DUPLICATE KEY UPDATE
                LOB_CODE               = '$LOB_CODE',
                LOB_NAME               = '$LOB_NAME',
                PROD_OFFERING_CODE     = '$PROD_OFFERING_CODE',
                PROD_OFFERING_NAME     = '$PROD_OFFERING_NAME',
                OFFICE_CODE            = '$OFFICE_CODE',
                OFFICE_NAME            = '$OFFICE_NAME',
                sync_status            = '1',
                sync_time              = now() ";
    if($resin =  mysqli_query($dbopen,$sqlin)){
        $idin1 = mysqli_insert_id($dbopen);
      
        $suc18 += $no;
    }else{
        $err18 += $no;
           //error 
           $sqlerrs = "INSERT INTO cc_log_sync_data_det SET
                        sync_desc   ='WISE_STAGING..V_POT_EXCLUDE',
                        sync_error  ='$SUPPL_CODE',
                        sync_time   =now()";
                mysqli_query($dbopen,$sqlerrs);
    }

}
//log 
$sqllog = "INSERT INTO cc_log_sync_data SET 
            sync_desc       ='WISE_STAGING..V_POT_EXCLUDE',
            sync_success    ='$suc18',
            sync_error      ='$err18',
            sync_time       =now()";
mysqli_query($dbopen,$sqllog);

echo "Sync Data WISE_STAGING..V_POT_EXCLUDE OK <br>";


echo "<br><br><br>";
echo "Sync All Data Complete !!!";
?>
