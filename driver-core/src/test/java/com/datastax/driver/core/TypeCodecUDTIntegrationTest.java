/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;

@CassandraVersion(major = 2.1)
public class TypeCodecUDTIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    private final String insertQuery = "INSERT INTO users (id, name, address) VALUES (?, ?, ?)";
    private final String selectQuery = "SELECT id, name, address FROM users WHERE id = ?";

    private final UUID uuid = UUID.randomUUID();

    private final Phone phone1 = new Phone("1234567", Sets.newHashSet("home", "iphone"));
    private final Phone phone2 = new Phone("2345678", Sets.newHashSet("work"));
    private final Address address = new Address("blah", 75010, Lists.newArrayList(phone1, phone2));

    private UDTValue addressValue;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TYPE IF NOT EXISTS \"phone\" (number text, tags set<text>)",
            "CREATE TYPE IF NOT EXISTS \"address\" (street text, zipcode int, phones list<frozen<phone>>)",
            "CREATE TABLE IF NOT EXISTS \"users\" (id uuid PRIMARY KEY, name text, address frozen<address>)"
        );
    }

    @BeforeMethod(groups = "short")
    public void setUp() throws Exception {
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
        UserType addressType = cluster.getMetadata().getKeyspace(keyspace).getUserType("address");
        UserType phoneType = cluster.getMetadata().getKeyspace(keyspace).getUserType("phone");
        UDTValue phone1Value = phoneType.newValue(protocolVersion, codecRegistry)
            .setString("number", phone1.number)
            .setSet("tags", phone1.tags);
        UDTValue phone2Value = phoneType.newValue(protocolVersion, codecRegistry)
            .setString("number", phone2.number)
            .setObject("tags", phone2.tags);
        addressValue = addressType.newValue(protocolVersion, codecRegistry)
            .setString("street", address.street)
            .setInt(1, address.zipcode)
            .setList("phones", Lists.newArrayList(phone1Value, phone2Value));
        TypeCodec<UDTValue> addressTypeCodec = new TypeCodec.UDTCodec(addressType, codecRegistry);
        TypeCodec<UDTValue> phoneTypeCodec = new TypeCodec.UDTCodec(phoneType, codecRegistry);
        codecRegistry
            .register(new AddressCodec(addressTypeCodec, Address.class, codecRegistry))
            .register(new PhoneCodec(phoneTypeCodec, Phone.class, codecRegistry))
        ;
    }

    @Test(groups = "short")
    public void should_handle_udts_with_default_codecs() {
        // simple statement
        session.execute(insertQuery, uuid, "John Doe", addressValue);
        ResultSet rows = session.execute(selectQuery, uuid);
        Row row = rows.one();
        assertRow(row);
        // prepared + values
        PreparedStatement ps = session.prepare(insertQuery);
        session.execute(ps.bind(uuid, "John Doe", addressValue));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with setUDTValue
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setUDTValue(2, addressValue));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with setObject + UDTValue
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setObject(2, addressValue));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with set + UDTValue
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").set(2, addressValue, UDTValue.class));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
    }

    @Test(groups = "short")
    public void should_handle_udts_with_custom_codecs() {
        // simple statement
        session.execute(insertQuery, uuid, "John Doe", address);
        ResultSet rows = session.execute(selectQuery, uuid);
        Row row = rows.one();
        assertRow(row);
        // prepared + values
        PreparedStatement ps = session.prepare(insertQuery);
        session.execute(ps.bind(uuid, "John Doe", address));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with setObject + Address type
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").setObject(2, address));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
        // bound with set + Address type
        session.execute(ps.bind().setUUID(0, uuid).setString(1, "John Doe").set(2, address, Address.class));
        rows = session.execute(selectQuery, uuid);
        row = rows.one();
        assertRow(row);
    }

    private void assertRow(Row row) {
        assertThat(row.getUUID(0)).isEqualTo(uuid);
        assertThat(row.getObject(0)).isEqualTo(uuid);
        assertThat(row.get(0, UUID.class)).isEqualTo(uuid);
        assertThat(row.getString(1)).isEqualTo("John Doe");
        assertThat(row.getObject(1)).isEqualTo("John Doe");
        assertThat(row.get(1, String.class)).isEqualTo("John Doe");
        assertThat(row.getUDTValue(2)).isEqualTo(addressValue);
        // corner case: getObject normally would use default codecs;
        // but tuple and udt codecs are registered on the fly
        // so if the user has manually registered a codec
        // for a specific tuple or udt, that one will be picked
        assertThat(row.getObject(2)).isEqualTo(address);
        assertThat(row.get(2, UDTValue.class)).isEqualTo(addressValue);
        assertThat(row.get(2, Address.class)).isEqualTo(address);
    }

    static class AddressCodec extends TypeCodec.MappingCodec<Address, UDTValue> {

        private final UserType addressType;

        private final CodecRegistry codecRegistry;

        public AddressCodec(TypeCodec<UDTValue> innerCodec, Class<Address> javaType, CodecRegistry codecRegistry) {
            super(innerCodec, javaType);
            addressType = (UserType) innerCodec.getCqlType();
            this.codecRegistry = codecRegistry;
        }

        @Override
        protected Address deserialize(UDTValue value) {
            return value == null ? null : new Address(value.getString("street"), value.getInt("zipcode"), value.getList("phones", Phone.class));
        }

        @Override
        protected UDTValue serialize(Address value) {
            return value == null ? null : addressType.newValue(ProtocolVersion.NEWEST_SUPPORTED, codecRegistry).setString("street", value.street).setInt("zipcode", value.zipcode).setList("phones", value.phones);
        }
    }

    static class PhoneCodec extends TypeCodec.MappingCodec<Phone, UDTValue> {

        private final UserType phoneType;

        private final CodecRegistry codecRegistry;

        public PhoneCodec(TypeCodec<UDTValue> innerCodec, Class<Phone> javaType, CodecRegistry codecRegistry) {
            super(innerCodec, javaType);
            this.codecRegistry = codecRegistry;
            phoneType = (UserType) innerCodec.getCqlType();
        }

        @Override
        protected Phone deserialize(UDTValue value) {
            return value == null ? null : new Phone(value.getString("number"), value.getSet("tags", String.class));
        }

        @Override
        protected UDTValue serialize(Phone value) {
            return value == null ? null : phoneType.newValue(ProtocolVersion.NEWEST_SUPPORTED, codecRegistry).setString("number", value.number).setSet("tags", value.tags);
        }
    }

    static class Address {

        String street;

        int zipcode;

        List<Phone> phones;

        public Address(String street, int zipcode, List<Phone> phones) {
            this.street = street;
            this.zipcode = zipcode;
            this.phones = phones;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Address address = (Address)o;
            return zipcode == address.zipcode && street.equals(address.street) && phones.equals(address.phones);
        }

        @Override
        public int hashCode() {
            int result = street.hashCode();
            result = 31 * result + zipcode;
            result = 31 * result + phones.hashCode();
            return result;
        }
    }

    static class Phone {

        String number;

        Set<String> tags;

        public Phone(String number, Set<String> tags) {
            this.number = number;
            this.tags = tags;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Phone phone = (Phone)o;
            return number.equals(phone.number) && tags.equals(phone.tags);
        }

        @Override
        public int hashCode() {
            int result = number.hashCode();
            result = 31 * result + tags.hashCode();
            return result;
        }
    }
}
