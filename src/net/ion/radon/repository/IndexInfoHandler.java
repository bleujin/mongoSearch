package net.ion.radon.repository;

import net.ion.nsearcher.config.Central;
import net.ion.nsearcher.reader.InfoReader;

public interface IndexInfoHandler<T> {

	T handle(SearchSession session, InfoReader infoReader);

}
