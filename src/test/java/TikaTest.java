import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.uneatlantico.*;
import com.uneatlantico.data.TikaAnalysis;

import org.apache.tika.exception.TikaException;
import org.junit.Test;

public class TikaTest {

	@Test
	public void Tikatest() throws IOException, TikaException {
		TikaAnalysis tika = new TikaAnalysis();
		List<String> lista = new ArrayList<>();
		lista.add("1");
		lista.add("2");
		lista.add("3");
		assertEquals(lista, tika.Palabras(tika.parseExample(new File("src\\main\\resources\\files\\dummy.txt"))).get(0));
	}

}
