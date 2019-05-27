import com.uneatlantico.data.Filtro;
import java.io.File;
import static org.junit.Assert.*;

import org.junit.Test;

public class FiltroTest {

	@Test
	public void testFiltro() {
		          File file = new File("src/main/resources/files");
                          for (final File fileEntry :  file.listFiles(new Filtro())) {
                              assertTrue(fileEntry.getName().equals("dummy.txt"));
                          }
	}

}
