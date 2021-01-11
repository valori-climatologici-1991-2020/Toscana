#10 gennaio 2021
rm(list=objects())
library("dplyr")
library("purrr")
library("readr")
library("magrittr")
library("stringr")
options(warn=-2)

#annoI viene determinato dopo prendendo min(yy)
annoI<-1000
annoF<-2020

creaCalendario<-function(annoI,annoF){
  
  if(missing(annoI)) stop("Anno inizio mancante")
  if(missing(annoF)) stop("Anno fine mancante")
  
  as.integer(annoI)->annoI
  as.integer(annoF)->annoF
  
  stopifnot(annoI<=annoF)
  
  seq.Date(from=as.Date(glue::glue("{annoI}-01-01")),to=as.Date(glue::glue("{annoF}-12-31")),by="day")->yymmdd
  
  tibble(yymmdd=yymmdd) %>%
    tidyr::separate(yymmdd,into=c("yy","mm","dd"),sep="-") %>%
    dplyr::mutate(yy=as.integer(yy),mm=as.integer(mm),dd=as.integer(dd))
  
}

read_delim("anagrafica.csv",delim=";",col_names=TRUE)->ana

purrr::map(ana$SiteCode,.f=function(codice){

	stringa<-glue::glue("http://www.sir.toscana.it/archivio/download.php?IDST=termo_csv&IDS={codice}")
	readLines(stringa)->testo
	length(testo)->numeroRighe

	#il server della Toscana IN OGNI CASO genera un file con l'intestazione del codice richiesto, anche se per
	#quel codice non esistono dati di precipitazione (ad esempio Calamazza, dati per solo water_level)
	#Quindi testo avrà sempre come minimo 19 righe
	if(numeroRighe<30){
		message(glue::glue("Stazione senza dati {codice}"))
		return()
	}#fine if

	#controlli paranoici per verificare i dati che stiamo scaricando
	stringaCod<-testo[2]
	#il codice della stazione non è quello riportato all'interno del file
	if(length(grep(codice,stringaCod,fixed=TRUE))!=1) stop(sprintf("Errore codice, stazione %s",codice))

	stringaHead<-testo[19]
	if(grep("gg/mm/aaaa",stringaHead,useBytes=TRUE)!=1) stop(glue::glue("Errore head - 1, stazione {codice}"))
	if(grep("Max",stringaHead,useBytes=TRUE)!=1) stop(glue::glue("Errore head - 2, stazione {codice}"))
	if(grep("Min",stringaHead,useBytes=TRUE)!=1) stop(glue::glue("Errore head - 2, stazione {codice}"))

	purrr::map_dfr(20:numeroRighe,.f=function(riga){

		unlist(str_split(testo[riga],";"))->valore
		unlist(str_split(valore[1],"/"))->ddmmaa

		#da dd mm aaaa -> aaaa mm dd		
		paste(ddmmaa[3],ddmmaa[2],ddmmaa[1],sep=";")->valore[1]

		#ora assegnamo -9999 (secondo la Toscana ci potrebbe comparire questo valore per indicare dati NA)
		#alla fine riconvertiamo tutto -9999 in NA 
		if(length(grep("-9999",valore[2]))) valore[2]<-"NA"
		if(length(grep("-9999",valore[3]))) valore[3]<-"NA"		
		if(length(grep(",",valore[2]))) valore[2]<-str_replace_all(valore[2],",",".")
		if(length(grep(",",valore[3]))) valore[3]<-str_replace_all(valore[3],",",".")

		unlist(str_split(valore[1],pattern=";"))->yymmdd
		tibble(yy=as.integer(yymmdd[1]),
		       mm=as.integer(yymmdd[2]),
		       dd=as.integer(yymmdd[3]),
		       Tmax=as.character(valore[2]),
		       Tmin=as.character(valore[3]))

	})->df.testo  #fine lapply
	
	
	which(ana$SiteCode==codice)->riga
	if(!length(riga)){
		sink("log_codiciNonTrovati.txt",append=TRUE)
		cat(paste0(codice,"\n"))
		sink()
		return()
	}

	if(length(riga)>1) stop(glue::glue("Trovato codice multiplo in anagrafica: {codice}"))
	df.testo %>%
		dplyr::select(yy,mm,dd,Tmax)->dfTmax
	names(dfTmax)[4]<-ana[riga,]$SiteID
	
	#write_delim(temp,delim=";",file=glue::glue("serie-{codice}-reg.toscanaTmax.csv"),col_names=TRUE)
	df.testo %>%dplyr::select(yy,mm,dd,Tmin)->dfTmin
	names(dfTmin)[4]<-ana[riga,]$SiteID
	#write_delim(temp,delim=";",file=glue::glue("serie-{codice}-reg.toscanaTmax.csv"),col_names=TRUE)
	
	list(dfTmax,dfTmin)
	
})->finale


purrr::compact(finale)->finale

if(!length(finale)) stop("Nessun dato in Toscana?")

purrr::map(finale,1) %>%
  purrr::reduce(.f=left_join,by=c("yy"="yy","mm"="mm","dd"="dd"))->Tmax

purrr::map(finale,2) %>%
  purrr::reduce(.f=left_join,by=c("yy"="yy","mm"="mm","dd"="dd"))->Tmin

min(Tmax$yy)->annoITmax
min(Tmin$yy)->annoITmin

min(annoITmax,annoITmin)->annoI

creaCalendario(annoI,annoF)->calendario

left_join(calendario,Tmax)->Tmax
left_join(calendario,Tmin)->Tmin

write_delim(Tmax,delim=";",file=glue::glue("Tmax_toscana.csv"),col_names=TRUE)
write_delim(Tmin,delim=";",file=glue::glue("Tmin_toscana.csv"),col_names=TRUE)
