#Query/Get data from sqlite databases and plot them

library(dplyr)
#library(dbplyr)
library(lubridate)

#source("/nwcdev/nwm_da/m3_dev/my_functions.R")  #on dw8
#source("/net/lfs0data6/nwm_da_m3_dev/my_functions.R") #on dw7

ifelse (dir.exists("/nwcdev/nwm_da/m3_dev/"), 
        source("/nwcdev/nwm_da/m3_dev/my_functions.R"),
        source("/net/lfs0data6/nwm_da/m3_dev/data_functions.R"))
        
#Main/MAIN program below

db_dir <- "/net/scratch/zzhang/m3db/western_us"
nwm_base_db_name <- paste0("nwm_ana_station_neighbor_archive_",
                          "2019100100_to_2020053123_base.db")

nwm_base_db_path <- file.path(db_dir, nwm_base_db_name)

#Process data based on csv files  -------------------------------------------------
ifelse (dir.exists("/nwcdev/nwm_da/m3_dev/"),
        data_path <- "/nwcdev/nwm_da/m3_dev",
        data_path <- "/net/lfs0data6/nwm_da/m3_dev")


nwm_swe_csv_fname <- "nwm_swe_2019100112_2020022912.csv"
wdb_swe_csv_fname <- "wdb_swe_2019100112_2020022912.csv"
nwm_csv_path <- file.path(data_path, nwm_swe_csv_fname)
wdb_csv_path <- file.path(data_path, wdb_swe_csv_fname)

from_date_num <- gsub("[^0-9]", "", nwm_swe_csv_fname) %>% substring(1,10)
to_date_num <- gsub("[^0-9]", "", nwm_swe_csv_fname) %>% substring(11,20)
data_fromDate <- date_period(from_date_num)
data_toDate <- date_period(to_date_num)

#Above info is for Western USA case only since we have already got the data


#Manually give the analysis period. If analyze whole period of data, 
# uncomment two lines below
fromDate <- "2019-10-01 12:00:00" 
toDate <- "2020-02-29 12:00:00"

if (!exists("fromDate") && !exists("toDate")) {
  fromDate <- data_fromDate
  toDate <- data_toDate
  subset_data <- FALSE
} else {
  subset_data <- TRUE
}

at_hour = 12 # Set to a nagetive or comment out will process every hour of data
             #Check if ready bias result data file exist (bias_result_*_*.csv)
             #This csv file should conresponding to the fromDate and toDate
hr_range <- 5 # maximum hour range away from the at_hour to use oberserved data


#Below will decide how to get the data: nwm_swe and wdb_swe

#Pre-determine file names for nwm and wdb data that have been processed
# according to the defined period, at_hour, and hr_range

file_paths <- get_nwm_wdb_bias_fnames(data_path,
                                      fromDate,
                                      toDate,
                                      at_hour,
                                      hr_range)
nwm_processed_path <- file_paths[[1]]
wdb_processed_path <- file_paths[[2]]
bias_file_path <- file_paths[[3]]
rm(file_paths)

if (file.exists(nwm_processed_path) & file.exists(wdb_processed_path)) {
  message("Reading procssed nwm/wdb csv files ...")
  nwm_com <- read.csv(nwm_processed_path)
  wdb_com <- read.csv(wdb_processed_path)
  message("\nCalculating statistics ...")
  result_df <- calculate_bias_stations(nwm_com, wdb_com)
} else {
  if (!file.exists(nwm_csv_path) | !file.exists(wdb_csv_path)) {
    # get data from python modules if there is no csv files available /////
    # For now, this only depends on if nwm/wdb_swe_csv_fname are given
    data_from_py <- get_data_via_py(data_fromDate, 
                                    data_toDate,
                                    nwm_base_db_path)
    nwm_swe <- data_from_py[[1]]
    wdb_swe <- data_from_py[[2]]
    #wdb_swe <- wdb_swe %>% select(-name, -recorded_elevation)
    #write these data to csv files to be used in the future
    write.csv(nwm_swe, nwm_swe_csv_fname, row.names = FALSE)
    write.csv(wdb_swe, wdb_swe_csv_fname, row.names = FALSE)
    rm(data_from_py)
    # end of getting data from python modules /////////////////////////////
    # Now the orginal data are available
  } else {
    message("Original data sets exist. Read them in ...")
    message("Reading whole NWM and WDB data in from csv files...")
    nwm_swe <- read.csv(nwm_csv_path)
    wdb_swe <- read.csv(wdb_csv_path)
    #wdb_swe <- wdb_swe %>% select(-name, -recorded_elevation) # get rid of these two columns
  }

  nwm_swe$datetime <- strptime(as.character(nwm_swe$datetime),
                               format="%Y-%m-%d %H:%M:%S")
  wdb_swe$datetime <- strptime(as.character(wdb_swe$datetime),
                               format="%Y-%m-%d %H:%M:%S")
  
  #t.fmt <- "%Y-%m-%d %H:%M:%S"
  #nwm_swe$datetime <- as.POSIXct(as.character(nwm_swe$datetime), format=t.fmt, tz="UTC")
  #wdb_swe$datetime <- as.POSIXct(as.character(wdb_swe$datetime), format=t.fmt, tz="UTC")
  
  ###NOTE on the datetime: If use strptime, it would be in the type of POSITlt, 
  #   there would be no issue of hour converting (from 12:00 - 18:00). 
  #   If use as.POSITct, the datetime would start at 18:00 (convert from 12:00to 18:00). 
  #   Both approaches will cause problems later on when try to filter the 
  #   data based on the datetime column.  But if there is no convertion
  #   of time (keep them as character), we can then use filter on datetime column

  #if((nchar(fromDate) == 0 | fromDate == NULL) && 
  #   (nchar(toDate) == 0 |toDate == NULL)) {
  if (subset_data) {
    #Subset data for the defined period
    message("\nSubsetting data for shorter period ...") #Subsetting data is expensive
    nwm_swe <- subset(nwm_swe, subset = datetime>=fromDate & datetime<=toDate)
    wdb_swe <- subset(wdb_swe, subset = datetime>=fromDate & datetime<=toDate)
    #nwm_sub <- subset(nwm_swe, subset = datetime>=fromDate & datetime<=toDate)
    #wdb_sub <- subset(wdb_swe, subset = datetime>=fromDate & datetime<=toDate)
  } else {
    message("Same period, no need to subset.")
    #No subsetting
    #nwm_sub <- nwm_swe
    #wdb_sub <- wdb_swe
    #rm(nwm_swe); rm(wdb_swe)
  }

  #Now both nwm and wdb data are ready for the defined period

  #Get data for those stations that exist in both datasets (but the length may different)
  nwm_in_wdb <- nwm_swe[nwm_swe$obj_identifier %in% wdb_swe$obj_identifier,]
  wdb_in_nwm <- wdb_swe[wdb_swe$obj_identifier %in% nwm_swe$obj_identifier,]
  #nwm_in_wdb <- nwm_sub[nwm_sub$obj_identifier %in% wdb_sub$obj_identifier,]
  #wdb_in_nwm <- wdb_sub[wdb_sub$obj_identifier %in% nwm_sub$obj_identifier,]

  #Note: May be need to convert datetime as as.character here if they are in POSIXct/POSIXlt
  #nwm_in_wdb$datetime <- as.character(nwm_in_wdb$datetime)
  #wdb_in_nwm$datetime <- as.character(wdb_in_nwm$datetime)
  #Now no need to convert because we need them as characters and they have not been converted 
  #  to POSIXct/POSIXlt

  #Convert datetime back to character
  nwm_in_wdb$datetime <- as.character(nwm_in_wdb$datetime)
  wdb_in_nwm$datetime <- as.character(wdb_in_nwm$datetime)
  
  #There are three options to calculate bias (return nwm_in_wdb and wdb_in_nwm):
  # 1) Based on all available hourly data for defined period
  # 2) Based on data sampled at the hour (at_hour) only
  # 3) Based on data sampled at the hour (at_hour). If the data at at_hour
  #   is missing,  search for the data at the nearest hour within the range hr_range
  # Note: It depends on hour at_hour and hr_range were given (negatives for no)

  #Check if nwm_wdb_com[[1]], nwm_wdb_com[[2]] exist before calling
  nwm_wdb_com <- sample_nwm_wdb_data(nwm_in_wdb, wdb_in_nwm, at_hour, hr_range)
  # returns a vector of two dataframes: nwm and wdb
  write.csv(nwm_wdb_com[[1]], nwm_processed_path, row.names = FALSE)
  write.csv(nwm_wdb_com[[2]], wdb_processed_path, row.names = FALSE)
  message("\nCalculating statistics ...")
  result_df <- calculate_bias_stations(nwm_wdb_com[[1]], nwm_wdb_com[[2]])
  
  # Write the result_df to a csv file for use later
  write.csv(result_df, bias_file_path, row.names = FALSE)
  
  #Snow accumulation and ablation analysis
  acc_abl_com <- accumulation_ablation_analysis(nwm_wdb_com[[1]], nwm_wdb_com[[2]])
  
  swe_acc_abl_com <- acc_abl_com[[1]]
  
  acc_stats <- acc_abl_com[[2]]
  acc_hit <- acc_abl_com[[3]]
  acc_miss <- acc_abl_com[[4]]
  
  abl_stats <- acc_abl_com[[5]]
  abl_hit <- acc_abl_com[[6]]
  abl_miss <- acc_abl_com[[7]]
  
  #write them to csv files
  acc_pre <- "acc_stats_"
  acc_stats_name <- get_time_related_fname(acc_pre,
                                           fromDate,
                                           toDate,
                                           at_hour,
                                           hr_range)
  write.csv(acc_stats, acc_stats_name, row.names = FALSE)
  
  abl_pre <- "abl_stats_"
  abl_stats_name <- get_time_related_fname(abl_pre,
                                           fromDate,
                                           toDate,
                                           at_hour,
                                           hr_range)
  write.csv(abl_stats, abl_stats_name, row.names = FALSE)
}
#***********************************************************

#Plot resultes below *********************************************************

####### Some common info below #####################
message("\nPlotting/Saving bias result ...")
library(ggmap)
library(ggplot2)
source("/nwcdev/nwm_da/m3_dev/plot_functions.R")
#define boundaries for Western USA
min_lat <- 30.0
max_lat <- 50.0 
min_lon <- -125.0
max_lon <- -100.0
map_width <- 7
map_height <- 7

if (at_hour < 0) {
  post <- paste0(".png")
} else if (at_hour >= 0 & hr_range <=0) {
  post <- paste0("_at", at_hour, "z.png")
} else {
  post <- paste0("_at", at_hour, "z_range", hr_range, "_h.png")
}

bg_map <- map_setup(data_ori, min_lat=min_lat, max_lat=max_lat,
                    min_lon=min_lon, max_lon=max_lon)
####### Some common info above #####################

#data_ori <- read.csv("bias_result_2019100112_2020022912.csv")
data_ori <- result_df
#rm(result_df)
data_sub <- subset(data_ori, subset = n_samples > 30) #sample size > 30
#data_lim <- subset(data_sub, subset = pbias >= -100 & pbias <= 100) #limit to (-100, 100)

val_breaks <- c(-Inf, -100, -60, -20, 20, 60, 100, Inf)  #for percent bias
color_breaks <- c('#800000', '#d7191c', '#fdae61', '#ffffbf', '#abd9e9', '#2c7bb6', '#162252')

plot_title <- "Modeled SWE Bias at Snow Reporter Sites"
if (exists("at_hour") && at_hour >= 0) {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, 
                          " To ", toDate, "] at hour ", at_hour, "z")
} else {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}

#___________________________________________________________________________
#Percent bias and distribution maps  -- similar to UCAR did
gg_plots <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                            size_var_coln="mean_swe", val_coln="pbias",
                            plot_title=plot_title, plot_subtitle=plot_subtitle,
                            x_label="Longitude", y_label="Latitude",
                            size_label="Mean\nSWE\n(mm)",
                            color_label="Bias (%)",
                            color_breaks,
                            size_min_pt=1, size_max_pt=10,
                            color_low="blue", color_mid="white", color_high="red",
                            val_size_min_lim=0, val_size_max_lim=800,
                            min_thresh_colr=-100, max_thresh_colr=100,
                            #exclVar="t_n", exclThresh=0.8,
                            val_breaks, alpha_val=0.8)


biasPlotName <- formFileName("bias_", fromDate, toDate, post)

ggsave(filename=biasPlotName, plot=gg_plots[[1]], units="in",
       width=map_width, height=map_height, dpi=300)

barPlotName <- formFileName("bias_distribution_", fromDate, toDate, post)
ggsave(filename=barPlotName, plot=gg_plots[[2]], units="in",
       width=6, height=4, dpi=300)
#___________________________________________________________________________
#
# ACCUMULATION HIT PLOTS

data_sub <- acc_hit
val_breaks <- c(-100, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, 100)
val_breaks <- c(0.01, 0.1, 0.25, 0.5, 0.75, 1.25, 2.0, 4.0, 10.0, 100.0)
val_breaks <- c(-Inf, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, Inf)
color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#E6FFE6',
                  '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
plot_title <- "SWE Aggregate Relative Acc Bias (AAB) Map"
if (exists("at_hour") && at_hour >= 0) {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, 
                          " To ", toDate, "] at hour ", at_hour, "z")
} else {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
gg_acc_hit <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                              size_var_coln="sum_obs_diff", val_coln="aab",
                              plot_title=plot_title, plot_subtitle=plot_subtitle,
                              x_label="Longitude", y_label="Latitude",
                              size_label="Sum\nObs swe diff\n(mm)",
                              color_label="AAB",
                              color_breaks,
                              size_min_pt=1, size_max_pt=10,
                              color_low="blue", color_mid="white", color_high="red",
                              val_size_min_lim=0, val_size_max_lim=1000,
                              min_thresh_colr=0, max_thresh_colr=1000,
                              #exclVar="t_n", exclThresh=0.8,
                              val_breaks, alpha_val=0.8)

mapPlotName <- formFileName("acc_hit_aab_map_", fromDate, toDate, post)

ggsave(filename=mapPlotName, plot=gg_acc_hit[[1]], units="in",
       width=map_width, height=map_height, dpi=300)

barPlotName <- formFileName("acc_hit_aab_distribution_", fromDate, toDate, post)
ggsave(filename=barPlotName, plot=gg_acc_hit[[2]], units="in",
       width=6, height=4, dpi=300)

# ACCUMULATION MISS PLOTS
data_sub <- acc_miss
plot_title <- "SWE Aggregate Relative Dryness Error Ratio (ADE) Map"
if (exists("at_hour") && at_hour >= 0) {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, 
                          " To ", toDate, "] at hour ", at_hour, "z")
} else {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
#val_breaks <- c(-0.01, -0.1, -0.25, -0.5, -0.75, -1.25, -2.0, -4.0, -10.0, -100.0)
gg_acc_miss <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                              size_var_coln="sum_obs_diff", val_coln="ade",
                              plot_title=plot_title, plot_subtitle=plot_subtitle,
                              x_label="Longitude", y_label="Latitude",
                              size_label="Sum\nObs swe diff\n(mm)",
                              color_label="ADE",
                              color_breaks,
                              size_min_pt=1, size_max_pt=10,
                              color_low="blue", color_mid="white", color_high="red",
                              val_size_min_lim=0, val_size_max_lim=1000,
                              min_thresh_colr=0, max_thresh_colr=1000,
                              #exclVar="t_n", exclThresh=0.8,
                              val_breaks, alpha_val=0.8)

mapPlotName <- formFileName("acc_miss_ade_map_", fromDate, toDate, post)

ggsave(filename=mapPlotName, plot=gg_acc_miss[[1]], units="in",
       width=map_width, height=map_height, dpi=300)

barPlotName <- formFileName("acc_miss_ade_distribution_", fromDate, toDate, post)
ggsave(filename=barPlotName, plot=gg_acc_miss[[2]], units="in",
       width=6, height=4, dpi=300)

# ABLATION HIT PLOTS
data_sub <- abl_hit
#val_breaks <- c(0.01, 0.1, 0.25, 0.5, 0.75, 1.25, 2.0, 4.0, 10.0, 100.0)
plot_title <- "SWE Aggregate Relative Ablation Bias (AMB) Map"
if (exists("at_hour") && at_hour >= 0) {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, 
                          " To ", toDate, "] at hour ", at_hour, "z")
} else {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
gg_abl_hit <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                              #size_var_coln="sum_obs_diff_p", val_coln="amb",
                              size_var_coln="sum_obs_diff", val_coln="amb",
                              plot_title=plot_title, plot_subtitle=plot_subtitle,
                              x_label="Longitude", y_label="Latitude",
                              size_label="Sum\nObs swe diff\n(mm)",
                              color_label="AMB",
                              color_breaks,
                              size_min_pt=1, size_max_pt=10,
                              color_low="blue", color_mid="white", color_high="red",
                              #val_size_min_lim=0, val_size_max_lim=1000,
                              val_size_min_lim=-1000, val_size_max_lim=0,
                              min_thresh_colr=0, max_thresh_colr=1000,
                              #exclVar="t_n", exclThresh=0.8,
                              val_breaks, alpha_val=0.8)

mapPlotName <- formFileName("abl_hit_amb_map_", fromDate, toDate, post)

ggsave(filename=mapPlotName, plot=gg_abl_hit[[1]], units="in",
       width=map_width, height=map_height, dpi=300)

barPlotName <- formFileName("abl_hit_amb_distribution_", fromDate, toDate, post)
ggsave(filename=barPlotName, plot=gg_abl_hit[[2]], units="in",
       width=6, height=4, dpi=300)

# ABLATION MISS PLOTS
data_sub <- abl_miss
plot_title <- "SWE Aggregate Relative Wetness Error Ratio (AWE) Map"
if (exists("at_hour") && at_hour >= 0) {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, 
                          " To ", toDate, "] at hour ", at_hour, "z")
} else {
  plot_subtitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
#val_breaks <- c(-0.01, -0.1, -0.25, -0.5, -0.75, -1.25, -2.0, -4.0, -10.0, -100.0)
gg_abl_miss <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                               size_var_coln="sum_obs_diff_p", val_coln="awe",
                               plot_title=plot_title, plot_subtitle=plot_subtitle,
                               x_label="Longitude", y_label="Latitude",
                               size_label="Sum\nObs swe diff\n(mm)",
                               color_label="AWE",
                               color_breaks,
                               size_min_pt=1, size_max_pt=10,
                               color_low="blue", color_mid="white", color_high="red",
                               val_size_min_lim=0, val_size_max_lim=1000,
                               min_thresh_colr=0, max_thresh_colr=1000,
                               #exclVar="t_n", exclThresh=0.8,
                               val_breaks, alpha_val=0.8)

mapPlotName <- formFileName("abl_miss_awe_map_", fromDate, toDate, post)

ggsave(filename=mapPlotName, plot=gg_abl_hit[[1]], units="in",
       width=map_width, height=map_height, dpi=300)

barPlotName <- formFileName("abl_miss_awe_distribution_", fromDate, toDate, post)
ggsave(filename=barPlotName, plot=gg_abl_hit[[2]], units="in",
       width=6, height=4, dpi=300)

#========================== below no longer needed

# #Bias vs. mean_swe scatter plot
# message("\nPlotting/Saving bias vs mean_swe scatter plot ...")
# ggplot(data=data_sub, aes(mean_swe, pbias)) +
#   geom_point(shape=21) +
#   scale_x_continuous(limits = c(0, 1000)) +
#   scale_y_continuous(limits = c(-100, 100))
# 
# #Bias vs. n_samples scatter plot
# message("\nPlotting/Saving bias vs n_samples scatter plot ...")
# ggplot(data=data_sub, aes(n_samples, pbias)) +
#   geom_point(shape=21) +
#   scale_x_continuous(limits = c(0, 800)) +
#   scale_y_continuous(limits = c(-100, 100))

color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#E6FFE6',
                  '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
val_breaks <- c(0.01, 0.1, 0.25, 0.5, 0.75, 1.25, 2.0, 4.0, 10.0, 100.0) # for ratio
#Plot swe accumulation and ablation bias maps

data_sub <- as.data.frame(acc_bias)
fillCol <- "b_cat"
valCol <- "acc_bias"
data_sub$b_cat <- cut(data_sub$acc_bias, breaks=valBreaks, right = TRUE)
valBreaksScaled <- scales::rescale(valBreaks,
                                   from=range(data_sub[,"acc_bias"],
                                              na.rm=TRUE,finite=TRUE))
colBreaks <- c('#800000', '#d7191c', '#fdae61', '#ffffbf', '#abd9e9', '#2c7bb6', '#162252')
bplotTitle <- "SWE Accumulation Bias"
if (exists("at_hour") && at_hour >= 0) {
  bplotSubTitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "] at hour ", at_hour)
} else {
  bplotSubTitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
colorLab = "Bias (%)"
#Bias Map
ggmap(bbox_map) +
  geom_point(aes_string(x=xCol, y=yCol, size=sizeCol, fill=fillCol),
             data=subset(data_sub, !is.na(data_sub$acc_bias)), alpha=alphaval, shape=21) +
  scale_size("Mean\nSWE\n(mm)", range=c(sizeMin,sizeMax), limits=c(0,valLim)) +
  scale_fill_manual(colorLab, values=colBreaks, drop=FALSE) +
  ggtitle(bquote(atop(.(bplotTitle), atop(italic(.(bplotSubTitle)), "")))) +
  labs(x = "Longitude", y = "Latitude") +
  theme(plot.title=element_text(size=18,face="bold", vjust=-1, hjust=0.5)) +
  guides(fill=guide_legend(override.aes=list(size=3), order=1), size=
           guide_legend(order=2))

#Bias distribution bar chart
ggplot(data=subset(data_sub, !is.na(data_sub$acc_bias)), aes(b_cat, fill=b_cat)) +
  geom_bar() +
  labs(x=colorLab, y="Site Count") +
  ggtitle(bquote(atop(.(paste0("Distribution of ", colorLab)),
                      atop(italic(.(bplotSubTitle)),"")))) +
  scale_fill_manual(colorLab, values=colBreaks, drop=F) +
  theme(plot.title=element_text(hjust=0.5)) +
  theme(axis.title.x = element_text(margin = margin(t=10,r=0,b=0,l=0))) +
  scale_x_discrete(drop=F) 
#---------------------------------------------------------------------------------
data_sub <- as.data.frame(ablation_bias)
fillCol <- "b_cat"
valCol <- "abl_bias"
data_sub$b_cat <- cut(data_sub$abl_bias, breaks=valBreaks, right = TRUE)
valBreaksScaled <- scales::rescale(valBreaks,
                                   from=range(data_sub[,"abl_bias"],
                                              na.rm=TRUE,finite=TRUE))
colBreaks <- c('#800000', '#d7191c', '#fdae61', '#ffffbf', '#abd9e9', '#2c7bb6', '#162252')
bplotTitle <- "SWE Ablation Bias"
if (exists("at_hour") && at_hour >= 0) {
  bplotSubTitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "] at hour ", at_hour)
} else {
  bplotSubTitle <- paste0("Western USA.  [From ", fromDate, " To ", toDate, "]")
}
colorLab = "Bias (%)"
#Bias Map
ggmap(bbox_map) +
  geom_point(aes_string(x=xCol, y=yCol, size=sizeCol, fill=fillCol),
             data=subset(data_sub, !is.na(data_sub$abl_bias)), alpha=alphaval, shape=21) +
  scale_size("Mean\nSWE\n(mm)", range=c(sizeMin,sizeMax), limits=c(0,valLim)) +
  scale_fill_manual(colorLab, values=colBreaks, drop=FALSE) +
  ggtitle(bquote(atop(.(bplotTitle), atop(italic(.(bplotSubTitle)), "")))) +
  labs(x = "Longitude", y = "Latitude") +
  theme(plot.title=element_text(size=18,face="bold", vjust=-1, hjust=0.5)) +
  guides(fill=guide_legend(override.aes=list(size=3), order=1), size=
           guide_legend(order=2))

#Bias distribution bar chart
ggplot(data=subset(data_sub, !is.na(data_sub$abl_bias)), aes(b_cat, fill=b_cat)) +
  geom_bar() +
  labs(x=colorLab, y="Site Count") +
  ggtitle(bquote(atop(.(paste0("Distribution of ", colorLab)),
                      atop(italic(.(bplotSubTitle)),"")))) +
  scale_fill_manual(colorLab, values=colBreaks, drop=F) +
  theme(plot.title=element_text(hjust=0.5)) +
  theme(axis.title.x = element_text(margin = margin(t=10,r=0,b=0,l=0))) +
  scale_x_discrete(drop=F) 
ggsave("ablation_swe_bias_distribution_2019100112_2020022912-at12_range5_h.png")



#---------------------------------------------------------------
ggplot(data=subset(data_sub, !is.na(data_sub$acc_bias)), aes(b_cat, fill=b_cat)) +
  geom_bar() +
  labs(x=colorLab, y="Site Count") +
  ggtitle(bquote(atop(.(paste0("Distribution of ", colorLab)),
                      atop(italic(.(bplotSubTitle)),"")))) +
  scale_fill_manual(colorLab, values=colBreaks, drop=F) +
  theme(plot.title=element_text(hjust=0.5)) +
  theme(axis.title.x = element_text(margin = margin(t=10,r=0,b=0,l=0))) +
  scale_x_discrete(drop=F) 

message("Done\n")

