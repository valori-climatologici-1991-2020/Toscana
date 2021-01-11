#10 gennaio 2021, revisione programma
rm(list=objects())
library("dplyr")
library("readr")
library("stringr")
library("purrr")
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


read_delim(file="anagrafica.csv",delim=";",col_names=TRUE)->ana

purrr::map(ana$SiteCode,.f=function(codice){

  
	stringa<-sprintf("http://www.sir.toscana.it/archivio/download.php?IDST=pluvio&IDS=%s",codice)
	readLines(stringa)->testo

	#Queste due righe sotto servono a catturare il caso in cui il codice stazione non e' nell'elenco
	#ma a volte questo controllo non funziona e non si capisce il perche
	#L'unico modo per ovviare a questo problema e' usare warn=-2 nel caso in cui il file mancante
	#venga catturato come un warning...comunque il programma funziona bene
	if(length(testo)==1)	if(grepl("doesn't exist",testo)) return() #il codice stazione non e' nell'elenco
	length(testo)->numeroRighe

	#il server della Toscana IN OGNI CASO genera un file con l'intestazione del codice richiesto, anche se per
	#quel codice non esistono dati di precipitazione (ad esempio Calamazza, dati per solo water_level)
	#Quindi testo avrà sempre come minimo 19 righe
	if(numeroRighe==19){
		print(sprintf("Stazione senza dati %s",codice))
	  return()
	}#fine if

	#print(length(testo))


	#controlli paranoici per verificare i dati che stiamo scaricando
	stringaCod<-testo[2]
	#il codice della stazione non è quello riportato all'interno del file
	if(length(grep(codice,stringaCod,fixed=TRUE))!=1) stop(sprintf("Errore codice, stazione %s",codice))

	stringaHead<-testo[19]
	if(grep("gg/mm/aaaa",stringaHead)!=1) stop(sprintf("Errore head - 1, stazione %s",codice))
	if(grep("Precipitazione",stringaHead)!=1) stop(sprintf("Errore head - 2, stazione %s",codice))

	CONTA<-0
	purrr::map(20:numeroRighe,.f=function(riga){

		unlist(str_split(testo[riga],";"))->valore
		unlist(str_split(valore[1],"/"))->ddmmaa

		#da dd mm aaaa -> aaaa mm dd		
		paste(ddmmaa[3],ddmmaa[2],ddmmaa[1],sep=";")->valore[1]

		#ora assegnamo -9999 (secondo la Toscana ci potrebbe comparire questo valore per indicare dati NA)
		#alla fine riconvertiamo tutto -9999 in NA 
		if(length(grep("@",valore[3]))) valore[2]<-"NA"
		if(length(grep("-9999",valore[2]))) valore[2]<-"NA"
		if(length(grep(",",valore[2]))) valore[2]<-str_replace_all(valore[2],",",".")

		if(valore[c(2)]==""){

			if(valore[c(3)]=="V"){
				valore[c(2)]<-"NA"
			}else if(valore[c(3)]=="P" | valore[c(3)]=="R"){
				valore[c(2)]<-"NA"
			}else{
				print(valore)
				stop("FLAG diverso da V/P/R per dato vuoto")
			}

		}#fine if

		CONTA<<-CONTA+1
		valore[c(1,2)]

	}) ->newTesto  #fine map
	
  if(!CONTA) return()
	
	#data frame
	purrr::map_chr(newTesto,1)->yymmdd
	purrr::map_chr(newTesto,2)->prec	
	
	tibble(yymmdd,prec)->df.testo

	df.testo %>%
	  tidyr::separate(col=yymmdd,into=c("yy","mm","dd"),sep=";") %>%
	  mutate(yy=as.integer(yy),mm=as.integer(mm),dd=as.integer(dd))->df.testo
	
	which(ana$SiteCode==codice)->riga
	
	if(!length(riga)){
	  sink("log_codiciNonTrovatiPrec.txt",append=TRUE)
	  cat(paste0(codice,"\n"))
	  sink()
	  return()
	}
	
	if(length(riga)>1) stop(glue::glue("Trovato codice multiplo in anagrafica: {codice}"))
	
	names(df.testo)[4]<-ana[riga,]$SiteID
	
	df.testo
	
})->finale

purrr::compact(finale)->finale
purrr::reduce(finale,.f=left_join,by=c("yy"="yy","mm"="mm","dd"="dd"))->Prec

min(Prec$yy)->annoI
creaCalendario(annoI,annoF)->calendario

left_join(calendario,Prec)->Prec
write_delim(Prec,delim=";",file=glue::glue("Prec_toscana.csv"),col_names=TRUE)

