package org.yottabase.lastfm.core;

import org.yottabase.lastfm.importer.ListenedTrack;
import org.yottabase.lastfm.importer.User;

public interface Driver {
	
	/**
	 * Inizializza il database 
	 */
	public void initializeSchema();
	
	/**
	 * Aggiunge un profilo al database
	 * @param user
	 */
	public void insertUser(User user);
	
	/**
	 * Aggiunge una traccia ascoltata al database
	 * @param listenedTrack
	 */
	public void insertListenedTrack(ListenedTrack listenedTrack);
	
	/**
	 * Conta il numero di artisti
	 */
	public void countArtists();
	
	/**
	 * Conta il numero di tracce
	 */
	public void countTracks();
	
	/**
	 * Conta il numero di utenti
	 */
	public void countUsers();
	
	/**
	 * Conta il numero complessivo di utenti, tracce ed artisti
	 */
	public void countEntities();
	
	/**
	 * Numero di canzoni mediamente ascoltate da un utente
	 * @param uniqueTrack specifica se si considerano solo ascolti di brani diversi
	 */
	public void averageNumberListenedTracksPerUser(boolean uniqueTrack);
	
	/**
	 * Numero di artisti mediamente ascoltati da un utente
	 * @param uniqueTrack specifica se si considerano solo ascolti di brani diversi
	 */
	public void averageNumberSungTracksPerArtist(boolean uniqueTrack);
	
	/**
	 * Gli utenti che hanno ascoltato più/meno canzoni
	 * @param n specifica quanti utenti selezionare (per numero decrescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 * @param uniqueTrack specifica se si considerano solo ascolti di brani diversi
	 */
	public void usersChart(int n, boolean top, boolean uniqueTrack);
	
	/**
	 * Le tracce più/meno ascoltate
	 * @param n specifica il numero di tracce da selezionare (per numero crescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 * @param uniqueTracks specifica se si considerano solo ascolti di brani diversi
	 */
	public void tracksChart(int n, boolean top, boolean uniqueTracks);
	
	/**
	 * Gli artisti più/meno ascoltati
	 * @param n specifica il numero di artisti da selezionare (per numero crescente di ascolti)
	 * @param top specifica il tipo di ordinamento (top->ASC, !top->DESC)
	 * @param uniqueTracks specifica se si considerano solo ascolti di brani diversi
	 */
	public void artistsChart(int n, boolean top, boolean uniqueTracks);
	
	/**
	 * Le top n tracce più ascoltate insieme
	 */
	public void tracksListenedTogether(int n);
	
	//TODO query con data
	
}
