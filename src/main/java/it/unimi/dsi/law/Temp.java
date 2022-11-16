package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.Sha256Hash;

import java.util.List;

public class Temp {
	public static void main (String[] args) {
		byte[] a = ByteConversion.concat(List.of(
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3),
				Sha256Hash.of(ByteConversion.long2bytes(5)).getBytes(), ByteConversion.long2bytes(3)
		));

		System.out.println(Sha256Hash.of(ByteConversion.long2bytes(5)));
		System.out.println(IncompleteMappings.parse(a));
	}
}
