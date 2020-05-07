/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Objects;

public class DDlogTBit extends DDlogType implements IsNumericType, IBoundedNumericType {
    private final int width;

    DDlogTBit(@Nullable Node node, int width, boolean mayBeNull) {
        super(node, mayBeNull);
        this.width = width;
    }

    @Override
    public String toString() {
        return this.wrapOption("bit<" + this.width + ">");
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        return new DDlogTBit(this.getNode(), this.width, mayBeNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDlogTBit dDlogTBit = (DDlogTBit) o;
        return width == dDlogTBit.width;
    }

    @Override
    public int hashCode() {
        return Objects.hash(width);
    }

    @Override
    public DDlogExpression zero() {
        return new DDlogEBit(this.getNode(), this.width, BigInteger.valueOf(0));
    }

    @Override
    public DDlogExpression one() {
        return new DDlogEBit(this.getNode(), this.width, BigInteger.valueOf(1));
    }

    @Override
    public String simpleName() {
        return "bit";
    }

    @Override
    public int getWidth() {
        return this.width;
    }

    @Override
    public IBoundedNumericType getWithWidth(int width) {
        return new DDlogTBit(this.getNode(), width, this.mayBeNull);
    }
}
