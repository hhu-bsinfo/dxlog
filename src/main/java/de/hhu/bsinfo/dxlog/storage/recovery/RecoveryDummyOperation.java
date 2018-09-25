/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxlog.storage.recovery;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.operations.Recovery;

/**
 * Dummy for DXMem recovery operation.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 24.09.2018
 */
public class RecoveryDummyOperation extends Recovery {

    /**
     * Constructor
     */
    public RecoveryDummyOperation() {
        super(null);
    }

    @Override
    public long createAndPutRecovered(final long[] p_cids, final long p_dataAddress, final int[] p_offsets,
            final int[] p_lengths, final int p_usedEntries) {
        return 0;
    }

    @Override
    public long createAndPutRecovered(final AbstractChunk... p_chunks) {
        return 0;
    }
}
