package net.ion.radon.repository;

import net.ion.isearcher.impl.Central;

public interface IndexInfoHandler<T> {

	T handle(SearchSession session, Central central);

}
