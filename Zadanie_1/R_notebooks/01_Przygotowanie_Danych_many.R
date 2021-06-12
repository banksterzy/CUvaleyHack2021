

for(month in c('05','04','03','02','01')){
  print(month)
  
  setwd(paste0("E:\\github\\2021_06_ValleyHack\\Zadanie1\\data_raw\\LK3 050L\\2020\\", month))
  
  pliki_spakowane <- list.files()
  
  dane_maszyna <- NULL
  
  tic()
  a <- map_df(pliki_spakowane, ~ unzip(.x) %>% as_tibble())
  
  b <- a %>% select(value) %>% pull
  b <- b[file.size(b) > 0]
  rm(a)
  print(1)
  myfiles = map(b, ~ read.csv(.x, header = FALSE, sep = "|", skip = 6))   
  rm(b)
  print(2)
  dane_start <- map(myfiles, ~ .x %>% 
                      select(-V2, -V5, -V7) %>% 
                      mutate(maszyna = str_sub(V1, start = 1, end = 17)) %>% 
                      mutate(zmienna = str_replace_all(V1, maszyna, "")) %>% 
                      select(-V1) %>% 
                      mutate(zmienna = gsub("_.*","",zmienna)) %>% 
                      rename(wartosc_zmiennej = V6,
                             data = V3,
                             czas = V4))
  rm(myfiles)
  print(3)
  dane_fin <- map(dane_start, ~ .x %>% 
                    pivot_wider(c(maszyna, data, czas), names_from = zmienna, values_from = wartosc_zmiennej, values_fn = max) %>% 
                    janitor::clean_names())
  rm(dane_start)
  print(4)
  dane_maszyna <- data.table::rbindlist(dane_fin, fill = TRUE)
  rm(dane_fin)
  toc()
  
  dane_maszyna <- dane_maszyna %>% select(-contains("e000"))
  write_parquet(dane_maszyna, paste0("E:\\github\\2021_06_ValleyHack\\Zadanie1\\data\\SILNIK_050L_2020_", month, ".parquet"))
  
  
}
  