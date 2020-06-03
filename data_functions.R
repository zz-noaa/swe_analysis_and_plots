# Functions to process swe data
################################ Functions Below 
connect_database <- function(base_db_path) {
  library(RSQLite)
  
  base <- DBI::dbConnect(RSQLite::SQLite(), base_db_path) 
  # call dbDisconnect(base) when it's done
  nwm_base_db_name <- basename(base_db_path)
  nwm_land_db_name <- gsub("base", "land_single", nwm_base_db_name)
  nwm_land_db_path <- file.path(db_dir, nwm_land_db_name)
  land <- DBI::dbConnect(RSQLite::SQLite(), nwm_land_db_path)
  # call dbDisconnect(land) when it's done
  
  #Get database info
  src_dbi(base)
  as.data.frame(dbListTables(base))  # or this
  src_dbi(land)
  as.data.frame(dbListTables(land))
  #To list fields of a table:
  as.data.frame(dbListFields(base, "databases_info"))
  # or
  dbListFields(base, "databases_info") #where databases_info is one of table names in base
  
  dbDisconnect(base) 
  dbDisconnect(land) 
}
#----------------------------------------------------
#---------------------------------------------------- 
get_nwm_wdb_bias_fnames <- function(data_path,
                                    fromDate,
                                    toDate,
                                    at_hour,
                                    hr_range) {
  nwm_pre <- "nwm_"
  nwm_processed_name <- get_time_related_fname(nwm_pre,
                                               fromDate,
                                               toDate,
                                               at_hour,
                                               hr_range)
  nwm_processed_path <- file.path(data_path, nwm_processed_name)
  
  wdb_pre <- "wdb_"
  wdb_processed_name <- get_time_related_fname(wdb_pre,
                                               fromDate,
                                               toDate,
                                               at_hour,
                                               hr_range)
  wdb_processed_path <- file.path(data_path, wdb_processed_name)
  
  #Determine the related result bias file name
  bias_pre <- "bias_result_"
  bias_file_name <- get_time_related_fname(bias_pre,
                                           fromDate,
                                           toDate,
                                           at_hour,
                                           hr_range)
  bias_file_path <- file.path(data_path, bias_file_name)
  fpaths_list <- list(nwm_processed_path,
                      wdb_processed_path,
                      bias_file_path)
  return (fpaths_list)
}
#----------------------------------------------------
#---------------------------------------------------- 

get_data_via_py <- function(fromDate, toDate, nwm_base_db_path) {
  library("reticulate", lib.loc="~/R/x86_64-redhat-linux-gnu-library/3.6")
  use_python("/usr/bin/python3.6")  #define which version of python to use
  source_python("query_nwm_wdb_data.py") #load the python modules
  nwm_wdb_data <- py_get_data_main(fromDate, toDate, nwm_base_db_path)
  nwm_data_py <- nwm_wdb_data[[1]]
  wdb_data_py <- nwm_wdb_data[[2]]
  #Convert datetime back to original as below
  nwm_data_py$datetime <- py_to_r(r_to_py(as.POSIXct(
    strptime(nwm_data_py$datetime,format="%Y-%m-%d %H:%M:%S"))))
  
  wdb_data_py$datetime <- py_to_r(r_to_py(as.POSIXct(
    strptime(wdb_data_py$datetime,format="%Y-%m-%d %H:%M:%S"))))
  
  return (list(nwm_data_py, wdb_data_py))
}
#----------------------------------------------------
#---------------------------------------------------- 
date_period <- function(date_num) {
  year <- date_num %>% substring(1 ,4)
  month <- date_num %>% substring(5, 6)
  day <- date_num %>% substring(7, 8)
  hour <- date_num %>% substring(9, 10)
  fromDate <- paste0(year, "-", month, "-", day, " ", hour, ":00:00")
}
#---------------------------------
#---------------------------------
get_time_related_fname <- function(pre,
                                   fromDate,
                                   toDate,
                                   at_hour,
                                   hr_range) {

  #pre <- "bias_result_"  
  if (at_hour < 0) {
    post <- paste0(".csv")
  } else if (at_hour >= 0 & hr_range <= 0) {
    post <- paste0("_at", at_hour, "z.csv")
  } else {
    post <- paste0("_at", at_hour, "z_range", hr_range, "h.csv")
  }
  bias_file_name <- formFileName(pre, fromDate, toDate, post)
  #subset_date <- FALSE
  return (bias_file_name)
}
#----------------------------------------------------
#---------------------------------------------------- 
formFileName <- function(pre, fromDate, toDate, post) {
  fname <- paste0(pre,
                  gsub("-", "",fromDate %>% substring(1,10)),
                  fromDate %>% substring(12,13),
                  "_",
                  gsub("-", "",toDate %>% substring(1,10)),
                  toDate %>% substring(12,13),
                  post)
}
#------------------------------------
#------------------------------------
pick_near_h_datetime <- function(df_each_day, at_hour, h_range){
  #Find the nearest datetime in this day to be picked
  h_min <- min(abs(hour(df_each_day$datetime)-at_hour))
  cell_loc <- which.min(abs(hour(df_each_day$datetime)-at_hour))
  datetime_cell <- df_each_day$datetime[cell_loc]
  if (h_min < h_range) {
    return (datetime_cell)
  } else {
    return ("")
  }
  #   h_alt <- at_hour + h_min
  #   near_dt <- paste0(df_each_day$ymd, sprintf("%02d", h_alt), ":00:00") #assuming ymd exists
  #   # near_dt <- paste0(year(df_each_day$datetime),
  #   #                   sprintf("%02d", month(df_each_day$datetime)),
  #   #                   sprintf("%02d", day(df_each_day$datetime)),
  #   #                   sprintf("%02d", h_alt),
  #   #                   ":00:00")
  # } else {
  #   near_dt <- ""  # no data exisit under the condition
  # }
  
  #return (near_dt)
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
sample_nwm_wdb_data <- function(nwm_in, wdb_in, at_hour=-1, hr_range=-1) {
  #sample data for common datetime between nwm_in and wdb_in depending on
  #values of at_hour, hr_range. nwm_in and wdb_in are having common stations now
  #before this function is called.
  #Default is including all hours of data
  
  `%notin%` <- Negate(`%in%`)
  
  #nwm_in <- nwm_in_wdb  #for local debug purpose
  #wdb_in <- wdb_in_nwm
  
  #check number of unique stations
  num_stations_nwm <- n_distinct(nwm_in$obj_identifier)
  num_stations_wdb <- n_distinct(wdb_in$obj_identifier)
  if (num_stations_nwm == num_stations_wdb) {
    print(paste0("Same number of stations for nwm and wdb --> ", 
                num_stations_wdb))
  } else {
    stop("The number of stations in two datasets does not match!")
    #warning("The number of stations in two datasets does not match!")
  }
  
  #Initializing two dataframes to hold final data of all stations
  nwm_com <- data.frame(obj_identifier = integer(),
                        datetime = character(),
                        swe = numeric())
  wdb_com <- data.frame(obj_identifier = integer(),
                        station_id = character(),
                        lon = numeric(),
                        lat = numeric(),
                        elevation = numeric(),
                        datetime = character(),
                        obs_swe_mm = numeric())
  # if (at_hour >= 0 && hr_range > 0) {
  #   #to define a tmp df to collect at or near hour entries
  #   wdb_g_at_and_near_h <- data.frame(obj_identifier = integer(),
  #                                 datetime = character()) #initializing
  #}

  station_count <- 0
  for(g in unique(nwm_in$obj_identifier)) {  #for each station
    station_count <- station_count + 1
    message("Processing data for ", g, " : ", station_count, 
            " of ", num_stations_wdb)
    #stop("the first station is ", g)
    nwm_group <- subset(nwm_in, subset = obj_identifier == g)
    wdb_group <- subset(wdb_in, subset = obj_identifier == g)
    
    if (at_hour < 0) {  #case 1
      #for the same station/group, find all the data for common datetimes
      nwm_com <- rbind(nwm_com, nwm_group[nwm_group$datetime %in%
                                            wdb_group$datetime,])
      wdb_com <- rbind(wdb_com, wdb_group[wdb_group$datetime %in%
                                            nwm_group$datetime,])
    } else if (at_hour >= 0 && hr_range <= 0) {  #case 2
      #find common data at the at_hour for all every day that exisit in wdb
      #One value each day if wdb@at_hour exist
      nwm_com <- rbind(nwm_com, nwm_group %>%
                         filter(hour(datetime) == at_hour)) 
      wdb_com <- rbind(wdb_com, wdb_group %>%
                         filter(hour(datetime) == at_hour)) 
    } else { # case 3: at_hour >= 0 && hr_range > 0
      #find common data at the at_hour plus at the nearest hour 
      #  if at_hour wdb data is missing
      
      #first find available at_hour data and will be temprorily 
      #  excluded in finding near hour
      wdb_g_at_hour <- wdb_group %>%
        filter(hour(datetime) == at_hour)
      
      #Add new column as ymd - a string of yyyymmdd, to be used in notin later
      wdb_group <- wdb_group %>%
        mutate(ymd=paste(format(as.Date(datetime,format="%Y-%m-%d"),
                                format = "%Y%m%d")))
      wdb_g_at_hour <- wdb_g_at_hour %>% 
        mutate(ymd=paste(format(as.Date(datetime,format="%Y-%m-%d"),
                                format = "%Y%m%d")))
      
      #append both at_hour and near_h data together
      #wdb_g_at_and_near_h <- rbind(wdb_g_at_and_near_h, wdb_g_at_hour)
      
      #wdb_com <- rbind(wdb_com, wdb_g_at_hour %>% select(-ymd))
      #wdb_g_at_near_h <- wdb_g_at_near_h %>% 
      #  mutate(ymd=paste(format(as.Date(datetime,format="%Y-%m-%d"), format = "%Y%m%d")))
      
      # create a tmp df to hold those that without at_hour data in these yyyymmdd
      wdb_g_near_h_tmp <- wdb_group[wdb_group$ymd %notin%
                                    wdb_g_at_hour$ymd, ]
      
      wdb_g_near_h <- slice(wdb_com, 0) #initializing
      
      #loop through all days and find the data at the nearest hour for each day
      for(ymdv in unique(wdb_g_near_h_tmp$ymd)) {
        wdb_g_near_h_each_day <- wdb_g_near_h_tmp %>% filter(ymd == ymdv)
        near_h_datetime <- pick_near_h_datetime(wdb_g_near_h_each_day,
                                                at_hour, hr_range)
        if (nchar(near_h_datetime) == 19) {
          wdb_g_near_h_d <- wdb_g_near_h_each_day %>%
            filter(datetime == near_h_datetime) %>% 
            select(-ymd)
          #wdb_g_at_and_near_h <- rbind(wdb_g_at_and_near_h, wdb_g_near_h_d)
          
          wdb_g_near_h <- rbind(wdb_g_near_h, wdb_g_near_h_d)
        }
      }
      
      wdb_com_g <- rbind(wdb_g_at_hour %>% select(-ymd), wdb_g_near_h)
      wdb_com_g <- wdb_com_g %>% arrange(datetime)
      wdb_com_g <- wdb_com_g[wdb_com_g$datetime %in% nwm_group$datetime, ] #make
      # sure that values in wdb are also in nwm
      wdb_com <- rbind(wdb_com, wdb_com_g)
      
      nwm_com_g <- nwm_group[nwm_group$datetime %in% wdb_com_g$datetime,]
      nwm_com <- rbind(nwm_com, nwm_com_g)
      # if (station_count == num_stations_wdb) {
      #   nwm_com <- nwm_in[nwm_in$datetime %in% wdb_com$datetime,]
      # }
      
    }  #end of case 3
    #stop("First station has finished for ", g)
  } # end of station/g loop
  
  data_com <- list(nwm_com, wdb_com)
  return (data_com)
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
calculate_bias_stations <- function(nwm_in_wdb, wdb_in_nwm, at_hour=-1) {
  #Calculate bias between two sets of data
  
  #check number of unique stations
  num_stations_nwm <- n_distinct(nwm_in_wdb$obj_identifier)
  num_stations_wdb <- n_distinct(wdb_in_nwm$obj_identifier)
  if (num_stations_nwm == num_stations_wdb) {
    print(paste0("Same number of stations for nwm and wdb --> ", 
                 num_stations_wdb))
  } else {
    stop("The number of stations in two datasets does not match!")
    #warning("The number of stations in two datasets does not match!")
  }
  
  
  #Loop through each station and calculate statistics
  #col_names <- c("obj_identifier", "lat","lon","pbias","mean_swe","n_samples")
  result_df <- data.frame(obj_identifier = integer(),
                          station_id = character(),
                          lat = numeric(),
                          lon = numeric(),
                          elevation = numeric(),
                          pbias = numeric(),
                          mean = numeric(),
                          n_samples = integer())  #Defining the dataframe 
  #result_df <- setNames(data.frame(matrix(ncol = 6, nrow = 0)), col_names)
  #num_unique_station <- length(unique(nwm_in_wdb$obj_identifier))
  
  station_count <- 0
  for(g in unique(nwm_in_wdb$obj_identifier)) {
    station_count <- station_count + 1
    message("Calcultaing bias for ", g, " : ", 
            station_count, " of ", num_stations_wdb)
    
    nwm_group <- subset(nwm_in_wdb, subset = obj_identifier == g)
    wdb_group <- subset(wdb_in_nwm, subset = obj_identifier == g)
    wdb_group <- wdb_group %>% select(obj_identifier, station_id,
                                        lon, lat, elevation, datetime, obs_swe_mm)
    
    wdb_group$nwm_swe <- nwm_group$swe
    wdb_group$swe_diff <- wdb_group$nwm_swe - wdb_group$obs_swe_mm
    
    if (sum(wdb_group$obs_swe_mm) != 0.0) {
      pbias <- 100*sum(wdb_group$swe_diff)/sum(wdb_group$obs_swe_mm)
      obj <- wdb_group$obj_identifier[1]
      sta_id <- wdb_group$station_id[1]
      elev <- wdb_group$elevation[1]
      lat <- wdb_group$lat[1]
      lon <- wdb_group$lon[1]
      
      mean_swe <- mean(wdb_group$obs_swe_mm, na.rm = T)
      n_samples <- nrow(wdb_group)
      #cat("obj=", obj, " pbias=", pbias, "\n")
      
      result_df <- rbind(result_df, 
                         data.frame(obj_identifier=obj,
                                    station_id=sta_id,
                                    lat=lat,
                                    lon=lon,
                                    elevation=elev,
                                    pbias=pbias,
                                    mean_swe=mean_swe,
                                    n_samples=n_samples))
    }
    
  } # end of g loop
  
  return(result_df)
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
accumulation_ablation_analysis <- function(nwm_com, wdb_com) {
  # #Determine the cases of accumulations and ablations for each obj_id
  # wdb_tmp <- wdb_com %>% filter(obj_identifier==11929 | obj_identifier==48099) %>% 
  #   group_by(obj_identifier) %>% mutate(wdb_swe_diff=obs_swe_mm-lag(obs_swe_mm))
  # 
  # nwm_tmp <- nwm_com %>% filter(obj_identifier==11929 | obj_identifier==48099) %>%
  #   group_by(obj_identifier) %>% mutate(nwm_swe_diff=swe-lag(swe))
  library(lubridate)
  
  wdb_com$datetime <- ymd_hms(wdb_com$datetime)
  wdb_tmp <- wdb_com %>% group_by(obj_identifier) %>% 
    mutate(wdb_swe_diff = case_when(month(datetime)==month(lag(datetime)) &
                                      (day(datetime)-day(lag(datetime)))==1
                                    ~ obs_swe_mm-lag(obs_swe_mm),
                                    (month(datetime)-month(lag(datetime)))==1 &
                                      day(datetime) == 1
                                    ~ obs_swe_mm-lag(obs_swe_mm),
                                    (month(datetime)-month(lag(datetime)))==-11 &
                                      day(datetime) == 1
                                    ~ obs_swe_mm-lag(obs_swe_mm),
                                    TRUE ~ NA_real_)) # deals with those that days 
                                                      # are not continuous etc
  
  nwm_com$datetime <- ymd_hms(nwm_com$datetime)
  nwm_tmp <- nwm_com %>% group_by(obj_identifier) %>%
    mutate(nwm_swe_diff = case_when(month(datetime)==month(lag(datetime)) &
                                      (day(datetime)-day(lag(datetime)))==1
                                    ~ swe-lag(swe),
                                    (month(datetime)-month(lag(datetime)))==1 &
                                      day(datetime) == 1
                                    ~ swe-lag(swe),
                                    (month(datetime)-month(lag(datetime)))==-11 &
                                      day(datetime) == 1
                                    ~ swe-lag(swe),
                                    TRUE ~ NA_real_))
  
  # nwm_tmp <- nwm_com %>% group_by(obj_identifier) %>% 
  #   mutate(nwm_swe_diff = swe-lag(swe)) 
  
  swe_acc_abl_com <- cbind((wdb_tmp %>% select(obj_identifier, station_id, lon, lat, elevation,
                                datetime, obs_swe_mm, wdb_swe_diff)), 
                   (nwm_tmp %>% select(swe, nwm_swe_diff)))
  
  swe_acc_abl_com <- within(swe_acc_abl_com, rm(obj_identifier1)) #get rid of another obj_ids
  swe_acc_abl_com <- swe_acc_abl_com %>% rename(nwm_swe=swe) #change name from swe to nwm_swe
  
  swe_acc_abl_com <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
    mutate(diff_ratio=nwm_swe/obs_swe_mm)
  
  #write to csv file for now  -- need a dynamic name
  #write.csv(swe_acc_abl_com, "swe_accum_ablation.csv", row.names = FALSE)
  rm(wdb_tmp, nwm_tmp)
  
  # ACCUMULATION HIT and MISS
  acc_stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% filter(wdb_swe_diff>0) %>%
    summarise(acc_bias=100*sum(nwm_swe-obs_swe_mm)/sum(obs_swe_mm),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm))
  
  acc_hit <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff>0 & nwm_swe_diff>0) %>% 
    summarise(mean_swediff_ratio=mean(nwm_swe_diff/wdb_swe_diff),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm))
  acc_hit_sum <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff>0 & nwm_swe_diff>0) %>% 
    summarise(sum_obs_diff=sum(wdb_swe_diff))
  acc_hit <- cbind(acc_hit, acc_hit_sum[, "sum_obs_diff"])
  rm(acc_hit_sum)
  
  # acc_hit_gmmb <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
  #   filter(wdb_swe_diff>0 & nwm_swe_diff>0) %>% 
  #   summarise(gmmb=10^mean(log10(nwm_swe_diff/wdb_swe_diff)))
  # acc_hit <- cbind(acc_hit, acc_hit_gmmb[, "gmmb"])
  # rm(acc_hit_gmmb)
  
  acc_hit_aab <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff>0 & nwm_swe_diff>0) %>% 
    summarise(aab=(sum(nwm_swe_diff)-sum(wdb_swe_diff))/sum(wdb_swe_diff))
  acc_hit <- cbind(acc_hit, acc_hit_aab[, "aab"])
  rm(acc_hit_aab)
  
  # acc_miss <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
  #   filter((wdb_swe_diff>0 & nwm_swe_diff<=0) | 
  #          (wdb_swe_diff>=0 & nwm_swe_diff<0)) %>% 
  #   summarise(mean_swediff_ratio=mean(nwm_swe_diff/wdb_swe_diff),
  #             lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm)) %>% 
  #   filter(!is.nan(mean_swediff_ratio))
  acc_miss <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter((wdb_swe_diff>0 & nwm_swe_diff<=0) | 
             (wdb_swe_diff>=0 & nwm_swe_diff<0)) %>% 
    summarise(ade=(sum(nwm_swe_diff)-sum(wdb_swe_diff))/sum(wdb_swe_diff),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm)) %>% 
    filter(!is.nan(ade) & !is.infinite(abs(ade)))
  # acc_miss <- cbind(acc_miss, acc_miss_ade[, "ade"])
  # rm(acc_miss_ade)
  
  acc_miss_sum <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter((wdb_swe_diff>0 & nwm_swe_diff<=0) | 
             (wdb_swe_diff>=0 & nwm_swe_diff<0)) %>%  
    summarise(sum_obs_diff=sum(wdb_swe_diff))
  #Find those that ade values are valid
  acc_miss_sum_tmp <- acc_miss_sum[acc_miss_sum$obj_identifier %in% 
                                 acc_miss$obj_identifier, ]
  acc_miss <- cbind(acc_miss, acc_miss_sum_tmp[, "sum_obs_diff"])
  rm(acc_miss_sum, acc_miss_sum_tmp)
  
  # acc_miss_gmmb <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
  #   filter(wdb_swe_diff>0 & nwm_swe_diff<0) %>% 
  #   summarise(gmmb=10^mean(log10(-nwm_swe_diff/wdb_swe_diff)))
  # acc_miss <- cbind(acc_miss, acc_miss_gmmb[, "gmmb"])
  # rm(acc_miss_gmmb
  

  
  #ABLATION HIT and MISS
  abl_stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% filter(wdb_swe_diff<0) %>%
    summarise(abl_bias=100*sum(nwm_swe-obs_swe_mm)/sum(obs_swe_mm),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm)) %>% 
    filter(mean_swe > 0)
  
  abl_hit <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff<0 & nwm_swe_diff<0) %>% 
    summarise(mean_swediff_ratio=mean(nwm_swe_diff/wdb_swe_diff),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm))
  abl_hit_sum <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff<0 & nwm_swe_diff<0) %>% 
    summarise(sum_obs_diff=sum(wdb_swe_diff))
  abl_hit <- cbind(abl_hit, abl_hit_sum[, "sum_obs_diff"])
  rm(abl_hit_sum)
  
  abl_hit_amb <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter(wdb_swe_diff<0 & nwm_swe_diff<0) %>% 
    summarise(amb=-(sum(nwm_swe_diff)-sum(wdb_swe_diff))/sum(wdb_swe_diff))
  abl_hit <- cbind(abl_hit, abl_hit_amb[, "amb"])
  rm(abl_hit_amb)
  abl_hit$sum_obs_diff_p <- -abl_hit$sum_obs_diff  # make positive for plotting
  
  #ABL MISS
  abl_miss <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter((wdb_swe_diff<0 & nwm_swe_diff>=0) |  
             (wdb_swe_diff<=0 & nwm_swe_diff>0)) %>% 
    summarise(awe=-(sum(nwm_swe_diff)-sum(wdb_swe_diff))/sum(wdb_swe_diff),
              lon=mean(lon), lat=mean(lat), mean_swe=mean(obs_swe_mm)) %>% 
    filter(!is.nan(awe) & !is.infinite(abs(awe)))
    
  abl_miss_sum <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
    filter((wdb_swe_diff<0 & nwm_swe_diff>=0) |  
             (wdb_swe_diff<=0 & nwm_swe_diff>0)) %>%
    summarise(sum_obs_diff=sum(wdb_swe_diff))
  abl_miss_sum_tmp <- abl_miss_sum[abl_miss_sum$obj_identifier %in% 
                                     abl_miss$obj_identifier, ]
  abl_miss <- cbind(abl_miss, abl_miss_sum_tmp[, "sum_obs_diff"])
  rm(abl_miss_sum, abl_miss_sum_tmp)
  abl_miss$sum_obs_diff_p <- -abl_miss$sum_obs_diff  # make positive for plotting
  
  # abl_miss_aaer <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
  #   filter(wdb_swe_diff<0 & nwm_swe_diff>0) %>% 
  #   summarise(aaer=sum(nwm_swe_diff)/sum(wdb_swe_diff))
  # abl_miss <- cbind(abl_miss, abl_miss_aaer[, "aaer"])
  # rm(abl_miss_aaer)
  
  return (list(swe_acc_abl_com, acc_stats, acc_hit, acc_miss,
               abl_stats, abl_hit, abl_miss))
}

# END OF FUNCTIONS--------------------------------------
