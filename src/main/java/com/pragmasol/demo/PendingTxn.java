/*
 * Copyright 2017 Pragmasol
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pragmasol.demo;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * UDT Class for Pending Transactions
 */
@UDT(name = "Pending")
public class PendingTxn {
    @Field
    public UUID transaction_id;
    @Field
    public BigDecimal amount;
    @Field
    public LocalDate valid_until;

}
