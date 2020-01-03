library(DBI)
library(tidyverse)


#pulling the list of all variables-----

con_prod <- odbc::dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server", 
                            Server = Sys.getenv("SERVER_PRODUCTION"), Database = Sys.getenv("DB_PD"), 
                            UID = Sys.getenv("UID"), PWD = Sys.getenv("PWD"))
table_id <- Id(schema = "surveys", 
               name = "all_variables")
all_vars <- dbReadTable(con_prod, name = table_id)
dbDisconnect(con_prod)

# section a1-----
con_st <- odbc::dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server", 
                          Server = Sys.getenv("SERVER_PRODUCTION"), Database = Sys.getenv("DB_ST"), 
                          UID = Sys.getenv("UID"), PWD = Sys.getenv("PWD"))

bsq <- dbReadTable(con_st, "bsq")

dbDisconnect(con_st)

section_a1 <- all_vars %>% 
  filter(grepl(x=family_name, "BSQ-Business Unit Characteristics")) %>% 
  select(variable_id, shortname)%>% 
  right_join(bsq) %>% 
  filter(!is.na(shortname)) %>% 
  select (-remote_unit_id, -variable_id) %>%
  spread(key = shortname, value= value)

table_id <- Id(schema = "organization", 
               name = "full_section_a1")

section_a1 %>% 
  mutate_if(is.character,
            ~stringi::stri_trans_general(., "latin-ascii")) %>%
  mutate_if(is.character,
            ~ stringr::str_replace_all(., "[^[:alnum:][:blank:]?&/\\-]", "")) -> section_a1
dbWriteTable(con_prod, table_id, section_a1,append = TRUE, overwrite = FALSE, 
             row.names=FALSE, encoding = "UTF-8")

# section e1-----
con_st <- odbc::dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server", 
                          Server = Sys.getenv("SERVER_PRODUCTION"), Database = Sys.getenv("DB_ST"), 
                          UID = Sys.getenv("UID"), PWD = Sys.getenv("PWD"))

bsq <- dbReadTable(con_st, "bsq")

dbDisconnect(con_st)

con_prod <- odbc::dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server", 
                            Server = Sys.getenv("SERVER_PRODUCTION"), Database = Sys.getenv("DB_PD"), 
                            UID = Sys.getenv("UID"), PWD = Sys.getenv("PWD"))
table_id <- Id(schema = "organization", 
               name = "characteristics")
characteristics <- dbReadTable(con_prod, name = table_id)
dbDisconnect(con_prod)



section_e1 <- all_vars %>% 
  filter(grepl(x=family_name, "BSQ/SCDS - Shared Variables on BSQ Faculty/Staff Counts & Staff Comp & Demog Survey")) %>% 
  select(variable_id, shortname)%>% 
  right_join(bsq) %>% 
  filter(!is.na(shortname)) %>% 
  select (-remote_unit_id, -variable_id) %>%
  spread(key = shortname, value= value) %>% 
  mutate(pf_unit_id = as.character(pf_unit_id)) %>% 
  left_join(characteristics, by = c("pf_unit_id" = "pf_unitid")) %>% 
  select(pf_unit_id, 171:177, everything(), -account_id)

table_id <- Id(schema = "organization", 
               name = "section_e1")

section_e1 %>% 
  mutate_if(is.character,
            ~stringi::stri_trans_general(., "latin-ascii")) %>%
  mutate_if(is.character,
            ~ stringr::str_replace_all(., "[^[:alnum:][:blank:]?&/\\-]", "")) -> section_e1

dbWriteTable(con_prod, table_id, section_e1, append = TRUE, overwrite = FALSE, 
             row.names=FALSE, encoding = "UTF-8", 
             field.types = c(deffacIP= "varchar(max)",
                             deffacPA= "varchar(max)",
                             deffacSA= "varchar(max)",
                             deffacSP= "varchar(max)"))

