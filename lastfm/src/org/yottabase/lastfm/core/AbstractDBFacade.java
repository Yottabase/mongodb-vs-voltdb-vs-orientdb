package org.yottabase.lastfm.core;

import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public abstract class AbstractDBFacade {
	
	protected OutputWriter writer = new ConsoleOutputWriter();
	
	public OutputWriter getWriter() {
		return writer;
	}

	public void setWriter(OutputWriter writer) {
		this.writer = writer;
	}

	/**
	 * Inizializza il database 
	 */
	public abstract void initializeSchema();
	
	/**
	 * Chiude il database
	 */
	public abstract void close();
	
	/**
	 * Aggiunge un profilo al database
	 * @param user
	 */
	public abstract void insertUser(User user);
	
	/**
	 * Aggiunge una traccia ascoltata al database
	 * @param listenedTrack
	 */
	public abstract void insertListenedTrack(ListenedTrack listenedTrack);
	
	/**
	 * Conta il numero di artisti
	 */
	public abstract void countArtists();
	
	/**
	 * Conta il numero di tracce
	 */
	public abstract void countTracks();
	
	/**
	 * Conta il numero di utenti
	 */
	public abstract void countUsers();
	
	/**
	 * Conta il numero complessivo di utenti, tracce ed artisti
	 */
	public abstract void countEntities();
	
	/**
	 * Numero di canzoni mediamente ascoltate da un utente
	 */
	public abstract void averageNumberListenedTracksPerUser();
	
	/**
	 * Numero di tracce mediamente cantate da un artista
	 */
	public abstract void averageNumberSungTracksPerArtist();
	
	/**
	 * Gli utenti che hanno ascoltato più/meno canzoni
	 * @param n specifica quanti utenti selezionare (per numero decrescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 */
	public abstract void usersChart(int n, boolean top);
	
	/**
	 * Le tracce più/meno ascoltate
	 * @param n specifica il numero di tracce da selezionare (per numero crescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 */
	public abstract void tracksChart(int n, boolean top);
	
	/**
	 * Gli artisti più/meno ascoltati
	 * @param n specifica il numero di artisti da selezionare (per numero crescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 */
	public abstract void artistsChart(int n, boolean top);
	
//	/**
//	 * Le top n tracce più ascoltate insieme
//	 */
//	public abstract void tracksListenedTogether(int n);
	
	/**
	 * L'artista corrispondente al codice
	 * @param artistCode è il codice dell'artista
	 */
	public abstract void artistByCode(String artistCode);
	
	/**
	 * Il primo artista corrispondente al nome
	 * @param artistName è il nome dell'artista
	 */
	public abstract void artistByName(String artistName);
	
	/**
	 * L'insieme degli utenti aventi età compresa nella fascia specificata
	 * @param lowerBound è l'età minima (inclusa)
	 * @param upperBound è l'età massima (inclusa)
	 */
	public abstract void usersByAgeRange(int lowerBound, int upperBound);
	
	/**
	 * La prima traccia ascoltata dall'utente specificato (rispetto ad ordine qualunque)
	 * @param userCode è il codice dell'utente
	 */
	public abstract void oneTrackListenedByUser(String userCode);
	
	/**
	 * Il coteggio degli utenti registrati per ogni differente paese
	 */
	public abstract void usersCountByCountry();
	
	/**
	 * Il coteggio degli utenti registrati per ogni differente paese
	 * divisi per sesso
	 */
	public abstract void usersCountByCountryAndGender();
	
}
