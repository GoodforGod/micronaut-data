package io.micronaut.data.document.tck.repositories;

import io.micronaut.data.annotation.Join;
import io.micronaut.data.document.tck.entities.Author;
import io.micronaut.data.document.tck.entities.AuthorBooksDto;
import io.micronaut.data.document.tck.entities.Book;
import io.micronaut.data.document.tck.entities.BookDto;
import io.micronaut.data.repository.PageableRepository;

import java.util.ArrayList;
import java.util.List;

public abstract class BookRepository implements PageableRepository<Book, String> {

    protected final AuthorRepository authorRepository;

    public BookRepository(AuthorRepository authorRepository) {
        this.authorRepository = authorRepository;
    }

    @Override
    @Join("author.books")
    public abstract Iterable<Book> findAll();

    public void saveAuthorBooks(List<AuthorBooksDto> authorBooksDtos) {
        List<Author> authors = new ArrayList<>();
        for (AuthorBooksDto dto: authorBooksDtos) {
            Author author = newAuthor(dto.getAuthorName());
            authors.add(author);
            for (BookDto book : dto.getBooks()) {
                newBook(author, book.getTitle(), book.getTotalPages());
            }
        }
        authorRepository.saveAll(authors);
    }

    protected Author newAuthor(String name) {
        Author author = new Author();
        author.setName(name);
        return author;
    }

    protected Book newBook(Author author, String title, int pages) {
        Book book = new Book();
        author.getBooks().add(book);
        book.setAuthor(author);
        book.setTitle(title);
        book.setTotalPages(pages);
        return book;
    }


}
